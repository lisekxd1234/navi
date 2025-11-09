from __future__ import annotations

import csv
import io
import json
import re
import shutil
import unicodedata
import uuid
import zipfile
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from pathlib import Path
from typing import List, Sequence, Tuple

from flask import (
    Flask,
    abort,
    flash,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    send_file,
    send_from_directory,
    url_for,
)
from flask_sqlalchemy import SQLAlchemy
from fpdf import FPDF
from fpdf.enums import XPos, YPos
from sqlalchemy import func, inspect, text
from werkzeug.utils import secure_filename

try:
    from .config import DEFAULT_ISSUE_PLACE, SELLER, UPLOAD_NDG, UPLOAD_ROOT
except ImportError:  # uruchomienie jako "python app/app.py"
    import sys

    current_dir = Path(__file__).resolve().parent
    if str(current_dir) not in sys.path:
        sys.path.append(str(current_dir))
    from config import DEFAULT_ISSUE_PLACE, SELLER, UPLOAD_NDG, UPLOAD_ROOT


# Stała limitu NDG - w razie zmiany można zaczytać z konfiguracji/ENV.
NDG_MONTHLY_LIMIT = Decimal("3499.50")
PDF_FONT_CANDIDATES = [
    Path(__file__).resolve().parent / "static" / "fonts" / "DejaVuSans.ttf",
    Path("C:/Windows/Fonts/arial.ttf"),
    Path("C:/Windows/Fonts/arialuni.ttf"),
    Path("C:/Windows/Fonts/segoeui.ttf"),
]
MONTH_NAMES_PL = [
    "styczeń",
    "luty",
    "marzec",
    "kwiecień",
    "maj",
    "czerwiec",
    "lipiec",
    "sierpień",
    "wrzesień",
    "październik",
    "listopad",
    "grudzień",
]


app = Flask(__name__)
app.config["SQLALCHEMY_DATABASE_URI"] = "sqlite:///finance.db"
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["SECRET_KEY"] = "zmien-to-na-losowe-haslo"

db = SQLAlchemy(app)

PDF_FONT_PATH = next((path for path in PDF_FONT_CANDIDATES if path.exists()), None)

UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
UPLOAD_NDG.mkdir(parents=True, exist_ok=True)
DB_PATH = Path(app.instance_path) / "finance.db"
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


class Invoice(db.Model):
    __tablename__ = "invoices"

    id = db.Column(db.Integer, primary_key=True)
    document_type = db.Column(db.String(20), nullable=False)  # faktura / rachunek
    number = db.Column(db.String(120), nullable=False)
    issue_date = db.Column(db.Date, nullable=False)
    sale_date = db.Column(db.Date)
    issue_place = db.Column(db.String(120))
    client_name = db.Column(db.String(255), nullable=False)
    client_tax_id = db.Column(db.String(50))
    client_address = db.Column(db.String(255))
    payment_method = db.Column(db.String(120))
    amount_paid = db.Column(db.Numeric(12, 2))
    items_json = db.Column(db.Text, nullable=False, default="[]")
    net_amount = db.Column(db.Numeric(12, 2), nullable=False)
    tax_rate = db.Column(db.Numeric(5, 2), nullable=False, default=Decimal("0"))
    gross_amount = db.Column(db.Numeric(12, 2), nullable=False)
    notes = db.Column(db.Text)
    internal_notes = db.Column(db.Text)

    @property
    def items(self) -> List[dict]:
        try:
            return json.loads(self.items_json)
        except json.JSONDecodeError:
            return []


class NDGDocument(db.Model):
    __tablename__ = "ndg_documents"

    id = db.Column(db.Integer, primary_key=True)
    number = db.Column(db.String(120), nullable=False)
    document_date = db.Column(db.Date, nullable=False)
    supplier_name = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text)
    amount = db.Column(db.Numeric(12, 2), nullable=False)
    file_reference = db.Column(db.String(255))
    internal_notes = db.Column(db.Text)
    attachments = db.relationship(
        "NDGAttachment", backref="document", cascade="all, delete-orphan", lazy="joined"
    )


class NDGAttachment(db.Model):
    __tablename__ = "ndg_attachments"

    id = db.Column(db.Integer, primary_key=True)
    document_id = db.Column(db.Integer, db.ForeignKey("ndg_documents.id"), nullable=False)
    file_reference = db.Column(db.String(255), nullable=False)


class ServiceTemplate(db.Model):
    __tablename__ = "service_templates"

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(120), nullable=False)
    description = db.Column(db.String(255), nullable=False)
    gross_price = db.Column(db.Numeric(12, 2), nullable=False)


@app.template_filter("pl_currency")
def pl_currency(value: Decimal | float | int | None) -> str:
    if value is None:
        return "0,00 zł"
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    value = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    formatted = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", " ")
    return f"{formatted} zł"


def initialize_database() -> None:
    db.create_all()
    _ensure_schema_updates()


@app.context_processor
def inject_globals():
    return {
        "NDG_MONTHLY_LIMIT": NDG_MONTHLY_LIMIT,
        "SELLER": SELLER,
        "DEFAULT_ISSUE_PLACE": DEFAULT_ISSUE_PLACE,
    }


def _ensure_schema_updates() -> None:
    inspector = inspect(db.engine)
    tables = inspector.get_table_names()

    if "invoices" in tables:
        columns = {col["name"] for col in inspector.get_columns("invoices")}
        if "internal_notes" not in columns:
            try:
                with db.engine.begin() as conn:
                    conn.execute(text("ALTER TABLE invoices ADD COLUMN internal_notes TEXT"))
            except Exception:
                app.logger.exception("Nie udało się dodać kolumny internal_notes do invoices.")
        for column in ["sale_date DATE", "issue_place TEXT", "payment_method TEXT", "amount_paid NUMERIC"]:
            col_name = column.split()[0]
            if col_name not in columns:
                try:
                    with db.engine.begin() as conn:
                        conn.execute(text(f"ALTER TABLE invoices ADD COLUMN {column}"))
                except Exception:
                    app.logger.exception("Nie udało się dodać kolumny %s do invoices.", col_name)
                else:
                    columns.add(col_name)

    if "ndg_documents" in tables:
        columns = {col["name"] for col in inspector.get_columns("ndg_documents")}
        if "internal_notes" not in columns:
            try:
                with db.engine.begin() as conn:
                    conn.execute(text("ALTER TABLE ndg_documents ADD COLUMN internal_notes TEXT"))
            except Exception:
                app.logger.exception("Nie udało się dodać kolumny internal_notes do ndg_documents.")


with app.app_context():
    initialize_database()


@app.route("/")
def index():
    return redirect(url_for("dashboard"))


@app.route("/dashboard")
def dashboard():
    today = date.today()
    month_labels, sales_data = _aggregate_monthly_sales(today, months_back=6)
    month_dates = [_month_key_to_date(label) for label in month_labels]
    ndg_labels, ndg_cost_values = _aggregate_ndg_documents(today, months_back=6)
    ndg_cost_map = dict(zip(ndg_labels, ndg_cost_values))
    ndg_costs = [ndg_cost_map.get(label, Decimal("0")) for label in month_labels]

    if month_dates:
        current_idx = len(month_dates) - 1
        current_date = month_dates[current_idx]
    else:
        current_idx = 0
        current_date = date(today.year, today.month, 1)

    current_month_sales = sales_data[current_idx] if sales_data else Decimal("0")
    current_month_usage = current_month_sales
    current_remaining = max(NDG_MONTHLY_LIMIT - current_month_usage, Decimal("0"))

    donut_dataset = [
        float(current_month_usage.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
        float(current_remaining.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)),
    ]

    history_rows = []
    for dt, sales_value, cost_value in reversed(
        list(zip(month_dates, sales_data, ndg_costs))
    ):
        history_rows.append(
            {
                "label": _month_name_pl(dt.year, dt.month, capitalize=False),
                "limit": NDG_MONTHLY_LIMIT,
                "sales": sales_value,
                "ndg": cost_value,
                "remaining": max(NDG_MONTHLY_LIMIT - sales_value, Decimal("0")),
            }
        )

    chart_sales_series = [float(val) for val in sales_data]
    chart_labels = [
        _month_name_pl(dt.year, dt.month, capitalize=True) for dt in month_dates
    ]
    chart_ndg_series = [float(val) for val in sales_data]
    chart_ndg_cost_series = [float(val) for val in ndg_costs]
    chart_limit_series = [float(NDG_MONTHLY_LIMIT) for _ in month_dates]
    chart_remaining_series = [
        float(max(NDG_MONTHLY_LIMIT - usage, Decimal("0"))) for usage in sales_data
    ]
    current_year = today.year
    annual_labels_raw, annual_totals = _annual_sales_by_month(current_year)
    annual_chart_labels = [
        _month_name_pl(current_year, month_idx + 1, capitalize=True)
        for month_idx in range(len(annual_totals))
    ]
    annual_chart_series = [float(amount) for amount in annual_totals]
    annual_sales_total = sum(annual_totals, Decimal("0"))

    usage_percent = 0.0
    if NDG_MONTHLY_LIMIT > 0:
        usage_percent = float((current_month_usage / NDG_MONTHLY_LIMIT) * 100)
        usage_percent = max(0.0, min(usage_percent, 100.0))

    return render_template(
        "dashboard.html",
        chart_labels=chart_labels,
        sales_series=chart_sales_series,
        ndg_series=chart_ndg_series,
        ndg_cost_series=chart_ndg_cost_series,
        limit_series=chart_limit_series,
        remaining_series=chart_remaining_series,
        annual_chart_labels=annual_chart_labels,
        annual_chart_series=annual_chart_series,
        annual_sales_total=annual_sales_total,
        current_month_name=_month_name_pl(
            current_date.year, current_date.month, capitalize=True
        ),
        current_year=current_year,
        current_limit=NDG_MONTHLY_LIMIT,
        current_sales=current_month_sales,
        ndg_usage=current_month_usage,
        ndg_remaining=current_remaining,
        usage_percent=usage_percent,
        donut_dataset=donut_dataset,
        history_rows=history_rows,
    )


@app.route("/api/dashboard-data")
def dashboard_data():
    today = date.today()
    month_labels, sales_data = _aggregate_monthly_sales(today, months_back=6)
    ndg_labels, ndg_cost_values = _aggregate_ndg_documents(today, months_back=6)
    ndg_cost_map = dict(zip(ndg_labels, ndg_cost_values))
    ndg_costs = [ndg_cost_map.get(label, Decimal("0")) for label in month_labels]

    current_usage = sales_data[-1] if sales_data else Decimal("0")
    current_remaining = max(NDG_MONTHLY_LIMIT - current_usage, Decimal("0"))

    return jsonify(
        {
            "labels": month_labels,
            "sales": [float(val) for val in sales_data],
            "ndg": [float(val) for val in sales_data],
            "ndg_costs": [float(val) for val in ndg_costs],
            "ndg_limit": float(NDG_MONTHLY_LIMIT),
            "current_ndg_usage": float(current_usage),
            "current_ndg_remaining": float(current_remaining),
        }
    )


@app.route("/api/next-number")
def next_number_api():
    raw_date = request.args.get("issue_date")
    doc_type = request.args.get("document_type", "faktura").lower()
    issue_date = _parse_any_date(raw_date) or date.today()
    if doc_type not in {"faktura", "paragon"}:
        doc_type = "faktura"
    number = _next_document_number(doc_type, issue_date)
    return jsonify({"number": number})


@app.route("/invoices")
def invoices():
    all_invoices = (
        Invoice.query.order_by(Invoice.issue_date.desc(), Invoice.number.desc()).all()
    )
    return render_template("invoices.html", invoices=all_invoices)


@app.route("/invoices/new", methods=["GET", "POST"])
def new_invoice():
    service_templates = ServiceTemplate.query.order_by(ServiceTemplate.name).all()
    template_options = [
        {
            "id": template.id,
            "name": template.name,
            "description": template.description,
            "gross_price": str(template.gross_price),
        }
        for template in service_templates
    ]
    prefill_items: List[dict] = []
    if request.method == "POST":
        try:
            issue_date = _parse_date(request.form.get("issue_date"))
            sale_date_raw = request.form.get("sale_date") or request.form.get("issue_date")
            sale_date = _parse_date(sale_date_raw) if sale_date_raw else None
            issue_place = request.form.get("issue_place", "").strip() or DEFAULT_ISSUE_PLACE
            document_type = request.form.get("document_type", "paragon").lower()
            if document_type not in {"faktura", "paragon"}:
                document_type = "paragon"
            number = request.form.get("number", "").strip()
            client_name = request.form.get("client_name", "").strip()
            client_tax_id = request.form.get("client_tax_id", "").strip()
            client_address = request.form.get("client_address", "").strip()
            tax_rate = _parse_decimal(request.form.get("tax_rate", "0"))
            notes = request.form.get("notes", "").strip() or None
            internal_notes = request.form.get("internal_notes", "").strip() or None
            payment_method = request.form.get("payment_method", "").strip() or "BLIK"
            amount_paid_raw = request.form.get("amount_paid", "").replace(",", ".")
            amount_paid = Decimal(amount_paid_raw) if amount_paid_raw else None
            if amount_paid is not None:
                amount_paid = amount_paid.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            if not number:
                number = _next_document_number(document_type, issue_date)
            if not issue_date:
                raise ValueError("Data wystawienia jest wymagana.")

            items = _extract_items_from_form(request, tax_rate)
            if not items:
                raise ValueError("Dodaj przynajmniej jedną pozycję.")

            net_total = sum((item["line_total_net"] for item in items), Decimal("0"))
            net_total = net_total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            gross_total = sum((item["line_total_gross"] for item in items), Decimal("0"))
            gross_total = gross_total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            if amount_paid is None:
                amount_paid = gross_total

            if not client_name:
                client_name = "Klient detaliczny"

            invoice = Invoice(
                document_type=document_type,
                number=number,
                issue_date=issue_date,
                sale_date=sale_date,
                issue_place=issue_place or DEFAULT_ISSUE_PLACE,
                client_name=client_name,
                client_tax_id=client_tax_id or None,
                client_address=client_address or None,
                payment_method=payment_method or "BLIK",
                amount_paid=amount_paid,
                items_json=json.dumps(
                    [
                        {
                            "description": item["description"],
                            "quantity": str(item["quantity"]),
                            "unit": item["unit"],
                            "unit_price_net": str(item["unit_price_net"]),
                            "unit_price_gross": str(item["unit_price_gross"]),
                            "line_total_net": str(item["line_total_net"]),
                            "line_total_gross": str(item["line_total_gross"]),
                        }
                        for item in items
                    ]
                ),
                net_amount=net_total,
                tax_rate=tax_rate,
                gross_amount=gross_total,
                notes=notes,
                internal_notes=internal_notes,
            )
            db.session.add(invoice)
            db.session.commit()
            flash("Dokument został zapisany. Możesz od razu pobrać PDF.", "success")
            return redirect(url_for("invoice_detail", invoice_id=invoice.id, saved=1))
        except (ValueError, InvalidOperation) as exc:
            flash(str(exc), "error")
            prefill_items = _prefill_from_request(request)

    today_str = date.today().isoformat()
    default_doc_type = "paragon"
    default_number = _next_document_number(default_doc_type, date.today())
    if request.method != "POST":
        prefill_items = []
    return render_template(
        "invoice_form.html",
        default_date=today_str,
        default_number=default_number,
        service_templates=service_templates,
        service_template_options=template_options,
        prefill_items=prefill_items,
        invoice=None,
        is_edit=False,
        auto_number_enabled=True,
        default_doc_type=default_doc_type,
        default_issue_place=DEFAULT_ISSUE_PLACE,
    )


@app.route("/invoices/<int:invoice_id>/edit", methods=["GET", "POST"])
def edit_invoice(invoice_id: int):
    invoice = Invoice.query.get_or_404(invoice_id)
    service_templates = ServiceTemplate.query.order_by(ServiceTemplate.name).all()
    template_options = [
        {
            "id": template.id,
            "name": template.name,
            "description": template.description,
            "gross_price": str(template.gross_price),
        }
        for template in service_templates
    ]
    prefill_items = [
        {
            "description": item["description"],
            "quantity": str(item["quantity"]),
            "gross_price": str(item["unit_price_gross"]),
        }
        for item in _parse_invoice_items(invoice)
    ]

    if request.method == "POST":
        try:
            issue_date = _parse_date(request.form.get("issue_date"))
            sale_date_raw = request.form.get("sale_date") or request.form.get("issue_date")
            sale_date = _parse_date(sale_date_raw) if sale_date_raw else None
            issue_place = request.form.get("issue_place", "").strip() or DEFAULT_ISSUE_PLACE
            document_type = request.form.get("document_type", invoice.document_type).lower()
            if document_type not in {"faktura", "paragon"}:
                document_type = invoice.document_type
            number = request.form.get("number", "").strip() or invoice.number
            client_name = request.form.get("client_name", "").strip()
            client_tax_id = request.form.get("client_tax_id", "").strip()
            client_address = request.form.get("client_address", "").strip()
            tax_rate = _parse_decimal(request.form.get("tax_rate", str(invoice.tax_rate or 0)))
            notes = request.form.get("notes", "").strip() or None
            internal_notes = request.form.get("internal_notes", "").strip() or None
            payment_method = request.form.get("payment_method", "").strip() or invoice.payment_method or "BLIK"
            amount_paid_raw = request.form.get("amount_paid", "").replace(",", ".")
            amount_paid = Decimal(amount_paid_raw) if amount_paid_raw else None
            if amount_paid is not None:
                amount_paid = amount_paid.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            if not number:
                number = invoice.number or _next_document_number(document_type, issue_date)
            if not issue_date:
                raise ValueError("Data wystawienia jest wymagana.")

            items = _extract_items_from_form(request, tax_rate)
            if not items:
                raise ValueError("Dodaj przynajmniej jedną pozycję.")

            net_total = sum((item["line_total_net"] for item in items), Decimal("0"))
            net_total = net_total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            gross_total = sum((item["line_total_gross"] for item in items), Decimal("0"))
            gross_total = gross_total.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            if amount_paid is None:
                amount_paid = gross_total

            if not client_name:
                client_name = "Klient detaliczny"

            invoice.document_type = document_type
            invoice.number = number
            invoice.issue_date = issue_date
            invoice.sale_date = sale_date
            invoice.issue_place = issue_place or DEFAULT_ISSUE_PLACE
            invoice.client_name = client_name
            invoice.client_tax_id = client_tax_id or None
            invoice.client_address = client_address or None
            invoice.payment_method = payment_method
            invoice.amount_paid = amount_paid
            invoice.items_json = json.dumps(
                [
                    {
                        "description": item["description"],
                        "quantity": str(item["quantity"]),
                        "unit": item["unit"],
                        "unit_price_net": str(item["unit_price_net"]),
                        "unit_price_gross": str(item["unit_price_gross"]),
                        "line_total_net": str(item["line_total_net"]),
                        "line_total_gross": str(item["line_total_gross"]),
                    }
                    for item in items
                ]
            )
            invoice.net_amount = net_total
            invoice.tax_rate = tax_rate
            invoice.gross_amount = gross_total
            invoice.notes = notes
            invoice.internal_notes = internal_notes

            db.session.commit()
            flash("Dokument został zaktualizowany.", "success")
            return redirect(url_for("invoice_detail", invoice_id=invoice.id))
        except (ValueError, InvalidOperation) as exc:
            flash(str(exc), "error")
            prefill_items = _prefill_from_request(request)

    return render_template(
        "invoice_form.html",
        default_date=invoice.issue_date.strftime("%Y-%m-%d"),
        default_number=invoice.number,
        service_templates=service_templates,
        service_template_options=template_options,
        prefill_items=prefill_items,
        invoice=invoice,
        is_edit=True,
        auto_number_enabled=False,
        default_issue_place=DEFAULT_ISSUE_PLACE,
    )


@app.post("/invoices/<int:invoice_id>/delete")
def delete_invoice(invoice_id: int):
    invoice = Invoice.query.get_or_404(invoice_id)
    db.session.delete(invoice)
    db.session.commit()
    flash("Dokument został usunięty.", "success")
    return redirect(url_for("invoices"))

@app.route("/invoices/<int:invoice_id>")
def invoice_detail(invoice_id: int):
    invoice = Invoice.query.get_or_404(invoice_id)
    parsed_items = _parse_invoice_items(invoice)
    tax_amount = invoice.gross_amount - invoice.net_amount
    amount_paid = invoice.amount_paid or Decimal("0")
    remaining_to_pay = max(invoice.gross_amount - amount_paid, Decimal("0"))
    amount_in_words = _number_to_words_pl(invoice.gross_amount)
    show_saved_modal = request.args.get("saved") == "1"
    document_title = _document_display_title(invoice)

    return render_template(
        "invoice_detail.html",
        invoice=invoice,
        items=parsed_items,
        tax_amount=tax_amount,
        amount_paid=amount_paid,
        amount_remaining=remaining_to_pay,
        amount_in_words=amount_in_words,
        show_saved_modal=show_saved_modal,
        document_display_title=document_title,
    )


@app.route("/invoices/<int:invoice_id>/pdf")
def invoice_pdf(invoice_id: int):
    invoice = Invoice.query.get_or_404(invoice_id)
    items = _parse_invoice_items(invoice)
    pdf_bytes = _invoice_pdf_bytes(invoice, items)
    filename = f"{invoice.document_type}_{invoice.number}".replace("/", "_").replace("\\", "_")

    response = make_response(pdf_bytes)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = f"attachment; filename={filename}.pdf"
    return response


@app.route("/invoices/export/pdf")
def export_invoices_pdf():
    invoices = Invoice.query.order_by(Invoice.issue_date).all()
    pdf_bytes = _sales_register_pdf_bytes(invoices)

    response = make_response(pdf_bytes)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = "attachment; filename=ewidencja_sprzedazy.pdf"
    return response


@app.route("/ndg")
def ndg_documents():
    documents = NDGDocument.query.order_by(
        NDGDocument.document_date.desc(), NDGDocument.number.desc()
    ).all()
    today = date.today()
    monthly_usage = _current_month_ndg_costs(today)

    ndg_labels_raw, ndg_series_raw = _aggregate_ndg_documents(today, months_back=12)
    ndg_chart_dates = [_month_key_to_date(label) for label in ndg_labels_raw]
    ndg_chart_labels = [
        _month_name_pl(dt.year, dt.month, capitalize=True) for dt in ndg_chart_dates
    ]
    ndg_chart_series = [float(val) for val in ndg_series_raw]

    monthly_rows = []
    for dt, total in zip(ndg_chart_dates, ndg_series_raw):
        monthly_rows.append(
            {
                "label": _month_name_pl(dt.year, dt.month, capitalize=True),
                "amount": total,
            }
        )
    ndg_total_window = sum(ndg_series_raw, Decimal("0"))

    return render_template(
        "ndg_documents.html",
        documents=documents,
        monthly_usage=monthly_usage,
        ndg_total_window=ndg_total_window,
        chart_labels=ndg_chart_labels,
        chart_series=ndg_chart_series,
        monthly_rows=monthly_rows,
    )


@app.route("/ndg/export/pdf")
def export_ndg_pdf():
    documents = NDGDocument.query.order_by(NDGDocument.document_date).all()
    pdf_bytes = _ndg_register_pdf_bytes(documents)

    response = make_response(pdf_bytes)
    response.headers["Content-Type"] = "application/pdf"
    response.headers["Content-Disposition"] = "attachment; filename=ndg_dokumenty.pdf"
    return response


@app.route("/services", methods=["GET", "POST"])
def service_templates_view():
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        description = request.form.get("description", "").strip()
        price_raw = request.form.get("gross_price", "0").replace(",", ".")

        if not name or not description:
            flash("Nazwa i opis szablonu są wymagane.", "error")
            return redirect(url_for("service_templates_view"))
        try:
            price = Decimal(price_raw)
        except InvalidOperation:
            flash("Niepoprawna kwota brutto.", "error")
            return redirect(url_for("service_templates_view"))

        template = ServiceTemplate(
            name=name,
            description=description,
            gross_price=price.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
        )
        db.session.add(template)
        db.session.commit()
        flash("Dodano szablon usługi.", "success")
        return redirect(url_for("service_templates_view"))

    templates = ServiceTemplate.query.order_by(ServiceTemplate.name).all()
    return render_template("service_templates.html", templates=templates)


@app.post("/services/<int:template_id>/delete")
def delete_service_template(template_id: int):
    template = ServiceTemplate.query.get_or_404(template_id)
    db.session.delete(template)
    db.session.commit()
    flash("Szablon został usunięty.", "success")
    return redirect(url_for("service_templates_view"))


@app.route("/uploads/<path:filename>")
def serve_upload(filename: str):
    try:
        full_path, relative = _safe_upload_path(filename)
    except FileNotFoundError:
        abort(404)
    if not full_path.exists():
        try:
            alt = filename.split("/", 1)[1]
            full_path, relative = _safe_upload_path(alt)
        except (IndexError, FileNotFoundError):
            abort(404)
        if not full_path.exists():
            abort(404)
    return send_file(full_path)


@app.route("/ndg/new", methods=["GET", "POST"])
def new_ndg_document():
    if request.method == "POST":
        try:
            doc_date = _parse_date(request.form.get("document_date"))
            number = request.form.get("number", "").strip()
            supplier_name = request.form.get("supplier_name", "").strip()
            description = request.form.get("description", "").strip()
            amount = _parse_decimal(request.form.get("amount", "0"))
            file_reference = request.form.get("file_reference", "").strip()
            internal_notes = request.form.get("internal_notes", "").strip() or None
            uploads = [
                file
                for file in request.files.getlist("attachments")
                if file and file.filename
            ]

            if not (doc_date and number and supplier_name):
                raise ValueError("Numer, data i dostawca są wymagane.")
            if amount <= 0:
                raise ValueError("Kwota musi być większa od zera.")

            document = NDGDocument(
                number=number,
                document_date=doc_date,
                supplier_name=supplier_name,
                description=description or None,
                amount=amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                file_reference=file_reference or None,
                internal_notes=internal_notes,
            )
            for upload in uploads:
                ref = _save_ndg_attachment(upload, number)
                document.attachments.append(NDGAttachment(file_reference=ref))
            db.session.add(document)
            db.session.commit()
            flash("Dokument NDG został zapisany.", "success")
            return redirect(url_for("ndg_documents"))
        except (ValueError, InvalidOperation) as exc:
            flash(str(exc), "error")

    default_date = date.today().isoformat()
    return render_template(
        "ndg_form.html",
        default_date=default_date,
        ndg_limit=NDG_MONTHLY_LIMIT,
        document=None,
        is_edit=False,
    )


@app.route("/ndg/<int:document_id>/edit", methods=["GET", "POST"])
def edit_ndg_document(document_id: int):
    document = NDGDocument.query.get_or_404(document_id)
    if request.method == "POST":
        try:
            doc_date = _parse_date(request.form.get("document_date"))
            number = request.form.get("number", "").strip()
            supplier_name = request.form.get("supplier_name", "").strip()
            description = request.form.get("description", "").strip()
            amount = _parse_decimal(request.form.get("amount", "0"))
            file_reference = request.form.get("file_reference", "").strip()
            internal_notes = request.form.get("internal_notes", "").strip() or None
            uploads = [
                file
                for file in request.files.getlist("attachments")
                if file and file.filename
            ]
            remove_ids = set(request.form.getlist("remove_attachment_ids"))

            if not (doc_date and number and supplier_name):
                raise ValueError("Numer, data i dostawca są wymagane.")
            if amount <= 0:
                raise ValueError("Kwota musi być większa od zera.")

            if remove_ids:
                for attachment in list(document.attachments):
                    if str(attachment.id) in remove_ids:
                        _delete_upload_files([attachment.file_reference])
                        db.session.delete(attachment)
            for upload in uploads:
                ref = _save_ndg_attachment(upload, number)
                document.attachments.append(NDGAttachment(file_reference=ref))

            document.number = number
            document.document_date = doc_date
            document.supplier_name = supplier_name
            document.description = description or None
            document.amount = amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            document.file_reference = file_reference or None
            document.internal_notes = internal_notes

            db.session.commit()
            flash("Dokument NDG został zaktualizowany.", "success")
            return redirect(url_for("ndg_documents"))
        except (ValueError, InvalidOperation) as exc:
            flash(str(exc), "error")

    return render_template(
        "ndg_form.html",
        default_date=document.document_date.strftime("%Y-%m-%d"),
        ndg_limit=NDG_MONTHLY_LIMIT,
        document=document,
        is_edit=True,
    )


@app.post("/ndg/<int:document_id>/delete")
def delete_ndg_document(document_id: int):
    document = NDGDocument.query.get_or_404(document_id)
    delete_refs: List[str] = []
    if document.file_reference:
        delete_refs.append(document.file_reference)
    delete_refs.extend([att.file_reference for att in document.attachments])
    _delete_upload_files(delete_refs)

    db.session.delete(document)
    db.session.commit()
    flash("Dokument NDG został usunięty.", "success")
    return redirect(url_for("ndg_documents"))


@app.post("/ndg/attachments/<int:attachment_id>/delete")
def delete_ndg_attachment(attachment_id: int):
    attachment = NDGAttachment.query.get_or_404(attachment_id)
    document_id = attachment.document_id
    _delete_upload_files([attachment.file_reference])
    db.session.delete(attachment)
    db.session.commit()
    flash("Załącznik został usunięty.", "success")
    return redirect(url_for("edit_ndg_document", document_id=document_id))


@app.post("/documents/purge")
def purge_documents():
    scope = (request.form.get("scope") or "all").lower()
    delete_sales = scope in {"sales", "sprzedaz", "sale", "all"}
    delete_ndg = scope in {"ndg", "koszty", "all"}

    deleted_sales = 0
    deleted_ndg = 0
    try:
        if delete_sales:
            deleted_sales = db.session.query(Invoice).delete(synchronize_session=False)
        if delete_ndg:
            refs = [
                ref
                for (ref,) in db.session.query(NDGDocument.file_reference).all()
                if ref
            ]
            _delete_upload_files(refs)
            deleted_ndg = db.session.query(NDGDocument).delete(synchronize_session=False)
        db.session.commit()
    except Exception:
        db.session.rollback()
        flash("Nie udało się usunąć dokumentów. Spróbuj ponownie.", "error")
        return redirect(url_for("import_data"))

    messages: List[str] = []
    if deleted_sales:
        messages.append(f"sprzedaży ({deleted_sales})")
    if deleted_ndg:
        messages.append(f"kosztów NDG ({deleted_ndg})")
    if not messages:
        flash("Brak danych do usunięcia.", "info")
    else:
        flash("Usunięto " + " i ".join(messages) + ".", "success")
    return redirect(url_for("import_data"))


def _delete_upload_files(references: Sequence[str]) -> None:
    for ref in references:
        if not ref:
            continue
        try:
            target, _ = _safe_upload_path(ref)
        except FileNotFoundError:
            continue
        try:
            if target.exists():
                target.unlink()
        except OSError:
            app.logger.warning("Nie udało się usunąć pliku %s", target)


def _read_csv_dicts(upload) -> List[dict]:
    content = upload.read()
    decoded: str
    if isinstance(content, bytes):
        decoded = _decode_csv_bytes(content)
    else:
        decoded = content
    upload.stream.seek(0)
    delimiter = _detect_csv_delimiter(decoded)
    reader = csv.DictReader(io.StringIO(decoded), delimiter=delimiter)
    rows: List[dict] = []
    for raw_row in reader:
        cleaned: dict[str, str] = {}
        for key, value in raw_row.items():
            key_clean = (key or "").strip()
            if not key_clean:
                continue
            cleaned[key_clean] = (value or "").strip()
        if cleaned:
            rows.append(cleaned)
    return rows


def _save_ndg_attachment(file_storage, preferred_name: str | None) -> str:
    filename = secure_filename(file_storage.filename)
    extension = Path(filename).suffix or ".bin"
    base_slug = _slugify(preferred_name or Path(filename).stem)
    if not base_slug:
        base_slug = uuid.uuid4().hex
    target_name = f"{base_slug}_{uuid.uuid4().hex[:8]}{extension}"
    target_path = (UPLOAD_NDG / target_name).resolve()
    target_path.parent.mkdir(parents=True, exist_ok=True)
    file_storage.save(target_path)
    relative = target_path.relative_to(UPLOAD_ROOT).as_posix()
    return relative


def _safe_upload_path(filename: str) -> Tuple[Path, Path]:
    cleaned = filename.replace("\\", "/")
    parts = [part for part in cleaned.split("/") if part and part not in {"..", "."}]
    if parts and parts[0].lower() == "uploads":
        parts = parts[1:]
    if not parts:
        raise FileNotFoundError("Brak ścieżki")
    relative = Path(*parts)
    full_path = (UPLOAD_ROOT / relative).resolve()
    if not str(full_path).startswith(str(UPLOAD_ROOT.resolve())):
        raise FileNotFoundError("Ścieżka poza katalogiem uploads")
    return full_path, relative


def _decode_csv_bytes(payload: bytes) -> str:
    encodings = ["utf-8-sig", "utf-8", "cp1250", "iso-8859-2", "latin-1"]
    for encoding in encodings:
        try:
            return payload.decode(encoding)
        except UnicodeDecodeError:
            continue
    return payload.decode("latin-1", errors="ignore")


def _detect_csv_delimiter(text_data: str) -> str:
    sample = text_data[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        return dialect.delimiter
    except csv.Error:
        if sample.count(";") > sample.count(","):
            return ";"
        return ","


def _row_get(row: dict, *keys: str) -> str | None:
    for key in keys:
        if not key:
            continue
        variants = {key, key.lower(), key.upper()}
        for variant in variants:
            if variant in row:
                value = (row[variant] or "").strip()
                if value:
                    return value
    return None


def _slugify(value: str) -> str:
    normalized = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
    )
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    return normalized.strip("_")


@app.route("/import")
def import_data():
    return render_template("import.html")


@app.route("/import/invoices", methods=["POST"])
def import_invoices_csv():
    file = request.files.get("csv_file")
    if not file or file.filename == "":
        flash("Wybierz plik CSV z danymi sprzedaży.", "error")
        return redirect(url_for("import_data"))

    rows = _read_csv_dicts(file)
    if not rows:
        flash("Plik CSV nie zawiera danych.", "error")
        return redirect(url_for("import_data"))

    imported = 0
    skipped = 0
    skipped_entries: List[tuple[str | None, str]] = []
    for row in rows:
        number = _row_get(row, "Numer", "Number", "No")
        if not number:
            skipped += 1
            skipped_entries.append((None, "Brak numeru dokumentu"))
            continue

        issue_date = _parse_any_date(_row_get(row, "Data", "Data dokumentu", "Issue Date")) or date.today()
        sale_date = _parse_any_date(_row_get(row, "Data sprzedaży", "Data Sprzedaży")) or issue_date
        document_type_raw = (_row_get(row, "Typ", "Type") or "faktura").strip().lower()
        if document_type_raw in {"pa", "par", "paragon"}:
            doc_type_clean = "paragon"
        elif document_type_raw in {"fhan", "fh", "faktura"}:
            doc_type_clean = "faktura"
        elif "rach" in document_type_raw:
            doc_type_clean = "rachunek"
        else:
            doc_type_clean = "faktura"

        if Invoice.query.filter_by(number=number, document_type=doc_type_clean).first():
            skipped += 1
            skipped_entries.append((number, "Numer już istnieje w bazie"))
            continue

        client_name = (
            _row_get(row, "Kontrahent", "Klient", "Client")
            or _row_get(row, "Odbiorca", "Buyer")
            or "Klient detaliczny"
        )
        payment_method = _row_get(row, "Sposób płatności", "Platnosc", "Payment Method")
        currency = (_row_get(row, "Waluta", "Currency") or "PLN").upper()

        gross_str = _row_get(row, "Wartość brutto", "Wartosc brutto", "Brutto", "Kwota")
        net_str = _row_get(row, "Wartość", "Wartosc", "Netto")
        try:
            gross_amount = _parse_decimal(gross_str or net_str or "0")
        except InvalidOperation:
            skipped += 1
            skipped_entries.append((number, "Niepoprawna kwota brutto"))
            continue
        try:
            net_amount = _parse_decimal(net_str) if net_str else gross_amount
        except InvalidOperation:
            net_amount = gross_amount
        tax_rate = Decimal("0")

        notes = _row_get(row, "Uwagi", "Notatki", "Notes") or None
        status = _row_get(row, "Status", "Stan")
        service_description = (
            _row_get(row, "Usługa", "Usluga", "Opis", "Opis zdarzenia")
            or notes
            or f"Pozycja {number}"
        )

        internal_notes_parts: List[str] = []
        if status:
            internal_notes_parts.append(f"Status: {status}")
        if currency and currency != "PLN":
            internal_notes_parts.append(f"Waluta: {currency}")
        internal_notes = "; ".join(internal_notes_parts) or None

        item_payload = [
            {
                "description": service_description,
                "quantity": "1",
                "unit": "usł.",
                "unit_price_net": str(net_amount),
                "unit_price_gross": str(gross_amount),
                "line_total_net": str(net_amount),
                "line_total_gross": str(gross_amount),
            }
        ]

        try:
            invoice = Invoice(
                document_type=doc_type_clean,
                number=number,
                issue_date=issue_date,
                sale_date=sale_date,
                issue_place=_row_get(row, "Miejsce wystawienia", "Miejsce"),
                client_name=client_name,
                client_address=None,
                payment_method=payment_method or None,
                amount_paid=None,
                items_json=json.dumps(item_payload),
                net_amount=net_amount,
                tax_rate=tax_rate,
                gross_amount=gross_amount,
                notes=notes,
                internal_notes=internal_notes,
            )
            db.session.add(invoice)
            imported += 1
        except Exception as exc:
            db.session.rollback()
            skipped += 1
            skipped_entries.append((number, f"Błąd zapisu: {exc}"))
            continue

    db.session.commit()
    if imported:
        flash(f"Zaimportowano {imported} dokumentów sprzedaży.", "success")
    if skipped_entries:
        details = "; ".join(
            f"{num or 'brak numeru'} ({reason})" for num, reason in skipped_entries[:10]
        )
        suffix = "… " if len(skipped_entries) > 10 else ""
        flash(
            f"Pominięto {skipped} dokumentów: {suffix}{details}",
            "warning",
        )
    return redirect(url_for("invoices"))


@app.route("/import/ndg", methods=["POST"])
def import_ndg_csv():
    csv_file = request.files.get("csv_file")
    if not csv_file or csv_file.filename == "":
        flash("Wybierz plik CSV z dokumentami NDG.", "error")
        return redirect(url_for("import_data"))

    rows = _read_csv_dicts(csv_file)
    if not rows:
        flash("Plik CSV nie zawiera danych.", "error")
        return redirect(url_for("import_data"))

    attachments_map: dict[str, zipfile.ZipInfo] = {}
    zip_file = request.files.get("attachments_zip")
    zip_stream = None
    if zip_file and zip_file.filename:
        try:
            zip_bytes = zip_file.read()
            zip_stream = zipfile.ZipFile(io.BytesIO(zip_bytes))
            for info in zip_stream.infolist():
                if info.is_dir():
                    continue
                if not info.filename.lower().endswith(".pdf"):
                    continue
                key = _slugify(Path(info.filename).stem)
                attachments_map[key] = info
        except zipfile.BadZipFile:
            flash("Niepoprawny plik ZIP z załącznikami.", "error")
            return redirect(url_for("import_data"))

    imported = 0
    skipped = 0
    attached = 0

    for row in rows:
        number = _row_get(row, "Numer", "Number", "No")
        if not number:
            skipped += 1
            continue
        existing = NDGDocument.query.filter_by(number=number).first()
        if existing:
            skipped += 1
            continue

        doc_date = _parse_any_date(_row_get(row, "Data", "Date")) or date.today()
        supplier_name = (
            _row_get(row, "Kontrahent", "Dostawca", "Supplier")
            or _row_get(row, "Odbiorca", "Buyer")
            or "Dostawca"
        )
        description = _row_get(row, "Uwagi", "Opis zdarzenia", "Opis", "Usługa", "Usluga")
        amount_str = _row_get(row, "Wartość brutto", "Wartosc brutto", "Wartość", "Wartosc", "Kwota", "Brutto")
        try:
            amount = _parse_decimal(amount_str or "0")
        except InvalidOperation:
            skipped += 1
            continue
        currency = (_row_get(row, "Waluta", "Currency") or "PLN").upper()
        status = _row_get(row, "Status", "Stan")
        internal_notes_parts: List[str] = []
        if status:
            internal_notes_parts.append(f"Status: {status}")
        if currency != "PLN":
            internal_notes_parts.append(f"Waluta: {currency}")

        ndg_doc = NDGDocument(
            number=number,
            document_date=doc_date,
            supplier_name=supplier_name,
            description=description or None,
            amount=amount.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
            file_reference=None,
            internal_notes="; ".join(internal_notes_parts) or None,
        )

        slug = _slugify(number)
        if slug and slug in attachments_map and zip_stream is not None:
            info = attachments_map[slug]
            target = UPLOAD_NDG / f"{slug}.pdf"
            with zip_stream.open(info) as source, open(target, "wb") as handle:
                handle.write(source.read())
            ndg_doc.file_reference = str(target.relative_to(UPLOAD_ROOT))
            attached += 1

        db.session.add(ndg_doc)
        imported += 1

    db.session.commit()
    if zip_stream is not None:
        zip_stream.close()
    flash(
        f"Zaimportowano {imported} dokumentów NDG (załączono {attached} plików). Pomięto {skipped}.",
        "success",
    )
    return redirect(url_for("ndg_documents"))


@app.route("/backup/export", methods=["GET"])
def export_database():
    if not DB_PATH.exists():
        flash("Plik bazy danych nie istnieje.", "error")
        return redirect(url_for("import_data"))
    filename = f"backup_{date.today():%Y%m%d}.db"
    return send_file(DB_PATH, as_attachment=True, download_name=filename)


@app.route("/backup/import", methods=["POST"])
def import_database_backup():
    uploaded = request.files.get("backup_file")
    if not uploaded or uploaded.filename == "":
        flash("Wybierz plik kopii bazy danych.", "error")
        return redirect(url_for("import_data"))

    tmp_path = DB_PATH.with_suffix(".upload")
    try:
        uploaded.save(tmp_path)
        db.session.remove()
        db.engine.dispose()
        shutil.copy2(tmp_path, DB_PATH)
        flash("Baza danych została przywrócona z kopii.", "success")
    except Exception as exc:
        flash(f"Nie udało się przywrócić bazy: {exc}", "error")
    finally:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
    return redirect(url_for("import_data"))


def _parse_decimal(raw_value: str | None) -> Decimal:
    if not raw_value:
        return Decimal("0")
    normalized = raw_value.replace(" ", "").replace(",", ".")
    return Decimal(normalized)


def _parse_date(raw_value: str | None) -> date:
    if not raw_value:
        raise ValueError("Data jest wymagana.")
    try:
        return datetime.strptime(raw_value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("Niepoprawny format daty. Użyj RRRR-MM-DD.") from exc


def _parse_any_date(raw_value: str | None) -> date | None:
    if not raw_value:
        return None
    candidates = [
        "%Y-%m-%d",
        "%d.%m.%Y",
        "%d/%m/%Y",
        "%Y.%m.%d",
    ]
    value = raw_value.strip()
    for fmt in candidates:
        try:
            return datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def _extract_items_from_form(req: request, tax_rate: Decimal) -> List[dict]:
    descriptions = req.form.getlist("item_description[]")
    quantities = req.form.getlist("item_quantity[]")
    gross_prices = req.form.getlist("item_gross_price[]")

    items = []
    for description, quantity, gross_price in zip(
        descriptions, quantities, gross_prices
    ):
        description = (description or "").strip()
        if not description:
            continue
        quantity_decimal = _parse_decimal(quantity or "1")
        unit_price_gross = _parse_decimal(gross_price or "0")
        if quantity_decimal <= 0:
            raise ValueError("Ilość pozycji musi być większa od zera.")
        if unit_price_gross < 0:
            raise ValueError("Cena jednostkowa nie może być ujemna.")

        tax_multiplier = tax_rate / Decimal("100") if tax_rate else Decimal("0")
        if tax_multiplier:
            unit_price_net = (unit_price_gross / (Decimal("1") + tax_multiplier)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        else:
            unit_price_net = unit_price_gross

        line_total_gross = (quantity_decimal * unit_price_gross).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        line_total_net = (quantity_decimal * unit_price_net).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        items.append(
            {
                "description": description,
                "quantity": quantity_decimal,
                "unit": "usł.",
                "unit_price_net": unit_price_net,
                "unit_price_gross": unit_price_gross.quantize(
                    Decimal("0.01"), rounding=ROUND_HALF_UP
                ),
                "line_total_net": line_total_net,
                "line_total_gross": line_total_gross,
            }
        )
    return items


def _parse_invoice_items(invoice: Invoice) -> List[dict]:
    parsed_items: List[dict] = []
    for raw_item in invoice.items:
        try:
            quantity = Decimal(str(raw_item.get("quantity", "0")))
        except InvalidOperation:
            quantity = Decimal("0")
        unit_price_net = _parse_decimal(str(raw_item.get("unit_price_net", raw_item.get("unit_price", "0"))))
        unit_price_gross = _parse_decimal(str(raw_item.get("unit_price_gross", raw_item.get("unit_price", "0"))))
        line_total_net = _parse_decimal(str(raw_item.get("line_total_net", raw_item.get("line_total", "0"))))
        line_total_gross = _parse_decimal(str(raw_item.get("line_total_gross", raw_item.get("line_total", "0"))))
        parsed_items.append(
            {
                "description": raw_item.get("description", ""),
                "quantity": quantity,
                "unit": raw_item.get("unit", "usł."),
                "unit_price_net": unit_price_net,
                "unit_price_gross": unit_price_gross,
                "line_total_net": line_total_net,
                "line_total_gross": line_total_gross,
            }
        )
    return parsed_items


def _prefill_from_request(req: request) -> List[dict]:
    descriptions = req.form.getlist("item_description[]")
    quantities = req.form.getlist("item_quantity[]")
    gross_prices = req.form.getlist("item_gross_price[]") or req.form.getlist("item_unit_price[]")

    prefill: List[dict] = []
    for description, quantity, gross_price in zip(descriptions, quantities, gross_prices):
        if not description:
            continue
        prefill.append(
            {
                "description": description,
                "quantity": quantity,
                "gross_price": gross_price,
            }
        )
    return prefill


def _aggregate_monthly_sales(reference: date, months_back: int = 6) -> Tuple[List[str], List[Decimal]]:
    month_bounds = [_month_bounds(reference, -offset) for offset in reversed(range(months_back))]
    labels = [start.strftime("%Y-%m") for start, _ in month_bounds]
    totals: List[Decimal] = []

    for start, end in month_bounds:
        total = (
            db.session.query(func.coalesce(func.sum(Invoice.gross_amount), 0))
            .filter(Invoice.issue_date >= start)
            .filter(Invoice.issue_date < end)
            .scalar()
        )
        totals.append(Decimal(total).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    return labels, totals


def _aggregate_ndg(reference: date, months_back: int = 6) -> Tuple[List[str], List[Decimal]]:
    month_bounds = [_month_bounds(reference, -offset) for offset in reversed(range(months_back))]
    labels = [start.strftime("%Y-%m") for start, _ in month_bounds]
    totals: List[Decimal] = []

    for start, end in month_bounds:
        ndg_total = (
            db.session.query(func.coalesce(func.sum(NDGDocument.amount), 0))
            .filter(NDGDocument.document_date >= start)
            .filter(NDGDocument.document_date < end)
            .scalar()
        )
        ndg_decimal = Decimal(str(ndg_total))
        combined = ndg_decimal + _sales_sum_between(start, end)
        totals.append(combined.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    return labels, totals


def _aggregate_ndg_documents(reference: date, months_back: int = 6) -> Tuple[List[str], List[Decimal]]:
    month_bounds = [_month_bounds(reference, -offset) for offset in reversed(range(months_back))]
    labels = [start.strftime("%Y-%m") for start, _ in month_bounds]
    totals: List[Decimal] = []

    for start, end in month_bounds:
        total = (
            db.session.query(func.coalesce(func.sum(NDGDocument.amount), 0))
            .filter(NDGDocument.document_date >= start)
            .filter(NDGDocument.document_date < end)
            .scalar()
        )
        totals.append(Decimal(str(total)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    return labels, totals


def _annual_sales_by_month(year: int) -> Tuple[List[str], List[Decimal]]:
    labels: List[str] = []
    totals: List[Decimal] = []
    for month in range(1, 13):
        start = date(year, month, 1)
        next_year, next_month = _shift_month(year, month, 1)
        end = date(next_year, next_month, 1)
        total = (
            db.session.query(func.coalesce(func.sum(Invoice.gross_amount), 0))
            .filter(Invoice.issue_date >= start)
            .filter(Invoice.issue_date < end)
            .scalar()
        )
        labels.append(f"{year}-{month:02d}")
        totals.append(Decimal(str(total)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    return labels, totals


def _sales_sum_between(start: date, end: date) -> Decimal:
    total = (
        db.session.query(func.coalesce(func.sum(Invoice.gross_amount), 0))
        .filter(Invoice.issue_date >= start)
        .filter(Invoice.issue_date < end)
        .scalar()
    )
    return Decimal(str(total)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _month_bounds(reference: date, month_offset: int) -> Tuple[date, date]:
    target_year, target_month = _shift_month(reference.year, reference.month, month_offset)
    start = date(target_year, target_month, 1)
    next_year, next_month = _shift_month(target_year, target_month, 1)
    end = date(next_year, next_month, 1)
    return start, end


def _shift_month(year: int, month: int, delta: int) -> Tuple[int, int]:
    idx = (year * 12 + (month - 1)) + delta
    new_year = idx // 12
    new_month = idx % 12 + 1
    return new_year, new_month


def _current_month_ndg_usage(reference: date) -> Decimal:
    start, end = _month_bounds(reference, 0)
    total_docs = (
        db.session.query(func.coalesce(func.sum(NDGDocument.amount), 0))
        .filter(NDGDocument.document_date >= start)
        .filter(NDGDocument.document_date < end)
        .scalar()
    )
    ndg_decimal = Decimal(str(total_docs))
    combined = ndg_decimal + _sales_sum_between(start, end)
    return combined.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _current_month_ndg_costs(reference: date) -> Decimal:
    start, end = _month_bounds(reference, 0)
    total = (
        db.session.query(func.coalesce(func.sum(NDGDocument.amount), 0))
        .filter(NDGDocument.document_date >= start)
        .filter(NDGDocument.document_date < end)
        .scalar()
    )
    return Decimal(str(total)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _month_name_pl(year: int, month: int, capitalize: bool = True) -> str:
    name = MONTH_NAMES_PL[month - 1]
    if capitalize:
        name = name.capitalize()
    return f"{name} {year}"


def _month_key_to_date(label: str) -> date:
    year, month = map(int, label.split("-"))
    return date(year, month, 1)


def _next_document_number(document_type: str, issue_date: date) -> str:
    start, end = _month_bounds(issue_date, 0)
    numbers = (
        db.session.query(Invoice.number)
        .filter(Invoice.document_type == document_type)
        .filter(Invoice.issue_date >= start)
        .filter(Invoice.issue_date < end)
        .all()
    )
    highest = 0
    for (number,) in numbers:
        if not number:
            continue
        parts = number.split("/")
        if not parts:
            continue
        try:
            value = int(parts[0])
        except (ValueError, TypeError):
            continue
        highest = max(highest, value)
    next_number = highest + 1
    return f"{next_number}/{issue_date.month}/{issue_date.year}"


def _pdf_with_title(title: str | None = None) -> Tuple[FPDF, str]:
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    font_family = "Helvetica"

    if PDF_FONT_PATH and PDF_FONT_PATH.exists():
        try:
            font_path = str(PDF_FONT_PATH)
            pdf.add_font("DocumentFont", "", font_path)
            pdf.add_font("DocumentFont", "B", font_path)
            font_family = "DocumentFont"
        except Exception:
            app.logger.warning("Nie udało się załadować czcionki %s.", PDF_FONT_PATH)

    title_text = (title or "").strip()
    if title_text:
        pdf.set_font(font_family, size=16)
        pdf.cell(0, 10, title_text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(4)
    pdf.set_font(font_family, size=11)
    return pdf, font_family


def _format_currency(value: Decimal | float | int) -> str:
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    value = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    formatted = f"{value:,.2f}".replace(",", " ").replace(".", ",")
    return f"{formatted} zł"


def _format_quantity(value: Decimal | float | int) -> str:
    if not isinstance(value, Decimal):
        value = Decimal(str(value or "0"))
    normalized = value.normalize()
    as_string = format(normalized, "f")
    if "." in as_string:
        as_string = as_string.rstrip("0").rstrip(".")
    return as_string.replace(".", ",") or "0"


def _pdf_table_row(
    pdf: FPDF,
    widths: Sequence[float],
    values: Sequence[str],
    *,
    line_height: float = 6,
    aligns: Sequence[str] | None = None,
) -> None:
    aligns = list(aligns or ["L"] * len(widths))
    y_start = pdf.get_y()
    x_start = pdf.get_x()
    max_lines = 1
    prepared_lines: List[List[str]] = []

    for text, width in zip(values, widths):
        content = (text or "").strip()
        lines = pdf.multi_cell(
            width,
            line_height,
            content,
            dry_run=True,
            output="LINES",
        )
        if not lines:
            lines = [""]
        prepared_lines.append(lines)
        max_lines = max(max_lines, len(lines))

    pdf.set_xy(x_start, y_start)
    for idx, (lines, width) in enumerate(zip(prepared_lines, widths)):
        align = "L"
        if idx < len(aligns):
            align = aligns[idx]
        pdf.multi_cell(
            width,
            line_height,
            "\n".join(lines),
            border=1,
            align=align,
            new_x="RIGHT",
            new_y="TOP",
            max_line_height=line_height,
        )
    pdf.set_xy(x_start, y_start + line_height * max_lines)


def _pdf_output(pdf: FPDF) -> bytes:
    data = pdf.output()
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, bytes):
        return data
    try:
        return data.encode("latin-1")
    except UnicodeEncodeError:
        return data.encode("latin-1", "ignore")

def _normalize_decimal(value: Decimal | float | int) -> Decimal:
    if isinstance(value, Decimal):
        normalized = value
    else:
        normalized = Decimal(str(value))
    return normalized.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

def _format_currency_plain(value: Decimal | float | int) -> str:
    amount = _normalize_decimal(value)
    return f"{amount:,.2f}".replace(",", " " ).replace(".", ",")

def _format_currency(value: Decimal | float | int) -> str:
    return f"{_format_currency_plain(value)} z\u0142"


_UNITS = [
    ["zero", "jeden", "dwa", "trzy", "cztery", "pi\u0119\u0107", "sze\u015b\u0107", "siedem", "osiem", "dziewi\u0119\u0107"],
    ["", "jeden", "dwa", "trzy", "cztery", "pi\u0119\u0107", "sze\u015b\u0107", "siedem", "osiem", "dziewi\u0119\u0107"],
]
_TEENS = [
    "dziesi\u0119\u0107",
    "jedena\u015bcie",
    "dwana\u015bcie",
    "trzyna\u015bcie",
    "czterna\u015bcie",
    "pi\u0119tna\u015bcie",
    "szesna\u015bcie",
    "siedemna\u015bcie",
    "osiemna\u015bcie",
    "dziewi\u0119tna\u015bcie",
]
_TENS = [
    "",
    "",
    "dwadzie\u015bcia",
    "trzydzie\u015bci",
    "czterdzie\u015bci",
    "pi\u0119\u0107dziesi\u0105t",
    "sze\u015b\u0107dziesi\u0105t",
    "siedemdziesi\u0105t",
    "osiemdziesi\u0105t",
    "dziewi\u0119\u0107dziesi\u0105t",
]
_HUNDREDS = [
    "",
    "sto",
    "dwie\u015bcie",
    "trzysta",
    "czterysta",
    "pi\u0119\u0107set",
    "sze\u015b\u0107set",
    "siedemset",
    "osiemset",
    "dziewi\u0119\u0107set",
]
_GROUPS = [
    ("z\u0142oty", "z\u0142ote", "z\u0142otych"),
    ("tysi\u0105c", "tysi\u0105ce", "tysi\u0119cy"),
    ("milion", "miliony", "milion\u00f3w"),
    ("miliard", "miliardy", "miliard\u00f3w"),
]
_GROSZ_FORMS = ("grosz", "grosze", "groszy")


def _declension(number: int, forms: tuple[str, str, str]) -> str:
    if number == 1:
        return forms[0]
    if 2 <= number % 10 <= 4 and not (12 <= number % 100 <= 14):
        return forms[1]
    return forms[2]


def _group_to_words(group: int, hide_one: bool) -> str:
    if group == 0:
        return ""
    words: list[str] = []
    hundreds = group // 100
    tens_units = group % 100
    tens = tens_units // 10
    units = tens_units % 10

    if hundreds:
        words.append(_HUNDREDS[hundreds])
    if tens_units:
        if tens_units < 10:
            if tens_units == 1 and hide_one and not hundreds:
                pass
            else:
                words.append(_UNITS[1][units])
        elif tens_units < 20:
            words.append(_TEENS[tens_units - 10])
        else:
            words.append(_TENS[tens])
            if units:
                words.append(_UNITS[1][units])
    return " ".join(words).strip()


def _number_to_words_pl(value: Decimal) -> str:
    value = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    zloty = int(value)
    grosze = int((value * 100) % 100)

    if zloty == 0:
        words = "zero z\u0142otych"
    else:
        parts: list[str] = []
        groups: list[int] = []
        tmp = zloty
        while tmp > 0:
            groups.append(tmp % 1000)
            tmp //= 1000

        for idx, group in enumerate(groups):
            if group == 0:
                continue
            hide_one = idx > 0
            group_words = _group_to_words(group, hide_one)
            forms = _GROUPS[idx] if idx < len(_GROUPS) else _GROUPS[-1]
            part = f"{group_words} {_declension(group, forms)}".strip()
            parts.insert(0, part)
        words = " ".join(parts).strip()

    grosze_part = f"{grosze:02d} {_declension(grosze, _GROSZ_FORMS)}"
    return f"{words}, {grosze_part}".strip()


def _prefill_from_request(req: request) -> List[dict]:
    descriptions = req.form.getlist("item_description[]")
    quantities = req.form.getlist("item_quantity[]")
    gross_prices = req.form.getlist("item_gross_price[]") or req.form.getlist("item_unit_price[]")

    prefill: List[dict] = []
    for description, quantity, gross_price in zip(descriptions, quantities, gross_prices):
        if not description:
            continue
        prefill.append(
            {
                "description": description,
                "quantity": quantity,
                "gross_price": gross_price,
            }
        )
    return prefill


def _aggregate_monthly_sales(reference: date, months_back: int = 6) -> Tuple[List[str], List[Decimal]]:
    month_bounds = [_month_bounds(reference, -offset) for offset in reversed(range(months_back))]
    labels = [start.strftime("%Y-%m") for start, _ in month_bounds]
    totals: List[Decimal] = []

    for start, end in month_bounds:
        total = (
            db.session.query(func.coalesce(func.sum(Invoice.gross_amount), 0))
            .filter(Invoice.issue_date >= start)
            .filter(Invoice.issue_date < end)
            .scalar()
        )
        totals.append(Decimal(total).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    return labels, totals


def _aggregate_ndg(reference: date, months_back: int = 6) -> Tuple[List[str], List[Decimal]]:
    month_bounds = [_month_bounds(reference, -offset) for offset in reversed(range(months_back))]
    labels = [start.strftime("%Y-%m") for start, _ in month_bounds]
    totals: List[Decimal] = []

    for start, end in month_bounds:
        total = (
            db.session.query(func.coalesce(func.sum(NDGDocument.amount), 0))
            .filter(NDGDocument.document_date >= start)
            .filter(NDGDocument.document_date < end)
            .scalar()
        )
        totals.append(Decimal(total).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    return labels, totals


def _month_bounds(reference: date, month_offset: int) -> Tuple[date, date]:
    target_year, target_month = _shift_month(reference.year, reference.month, month_offset)
    start = date(target_year, target_month, 1)
    next_year, next_month = _shift_month(target_year, target_month, 1)
    end = date(next_year, next_month, 1)
    return start, end


def _shift_month(year: int, month: int, delta: int) -> Tuple[int, int]:
    idx = (year * 12 + (month - 1)) + delta
    new_year = idx // 12
    new_month = idx % 12 + 1
    return new_year, new_month


def _current_month_ndg_usage(reference: date) -> Decimal:
    start, end = _month_bounds(reference, 0)
    total = (
        db.session.query(func.coalesce(func.sum(NDGDocument.amount), 0))
        .filter(NDGDocument.document_date >= start)
        .filter(NDGDocument.document_date < end)
        .scalar()
    )
    return Decimal(total).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def _month_name_pl(year: int, month: int, capitalize: bool = True) -> str:
    name = MONTH_NAMES_PL[month - 1]
    if capitalize:
        name = name.capitalize()
    return f"{name} {year}"


def _month_key_to_date(label: str) -> date:
    year, month = map(int, label.split("-"))
    return date(year, month, 1)


def _next_document_number(document_type: str, issue_date: date) -> str:
    start, end = _month_bounds(issue_date, 0)
    numbers = (
        db.session.query(Invoice.number)
        .filter(Invoice.document_type == document_type)
        .filter(Invoice.issue_date >= start)
        .filter(Invoice.issue_date < end)
        .all()
    )
    highest = 0
    for (number,) in numbers:
        if not number:
            continue
        parts = number.split("/")
        if not parts:
            continue
        try:
            value = int(parts[0])
        except (ValueError, TypeError):
            continue
        highest = max(highest, value)
    next_number = highest + 1
    return f"{next_number}/{issue_date.month}/{issue_date.year}"


def _pdf_with_title(title: str | None = None) -> Tuple[FPDF, str]:
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    font_family = "Helvetica"

    if PDF_FONT_PATH and PDF_FONT_PATH.exists():
        try:
            font_path = str(PDF_FONT_PATH)
            pdf.add_font("DocumentFont", "", font_path)
            pdf.add_font("DocumentFont", "B", font_path)
            font_family = "DocumentFont"
        except Exception:
            app.logger.warning("Nie udało się załadować czcionki %s.", PDF_FONT_PATH)

    title_text = (title or "").strip()
    if title_text:
        pdf.set_font(font_family, size=16)
        pdf.cell(0, 10, title_text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(4)
    pdf.set_font(font_family, size=11)
    return pdf, font_family


def _format_currency(value: Decimal | float | int) -> str:
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    value = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    formatted = f"{value:,.2f}".replace(",", " ").replace(".", ",")
    return f"{formatted} zł"


def _pdf_output(pdf: FPDF) -> bytes:
    data = pdf.output(dest="S")
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, bytes):
        return data
    try:
        return data.encode("latin-1")
    except UnicodeEncodeError:
        return data.encode("latin-1", "ignore")


def _prefill_from_request(req: request) -> List[dict]:
    descriptions = req.form.getlist("item_description[]")
    quantities = req.form.getlist("item_quantity[]")
    gross_prices = req.form.getlist("item_gross_price[]") or req.form.getlist("item_unit_price[]")

    prefill: List[dict] = []
    for description, quantity, gross_price in zip(descriptions, quantities, gross_prices):
        if not description:
            continue
        prefill.append(
            {
                "description": description,
                "quantity": quantity,
                "gross_price": gross_price,
            }
        )
    return prefill


def _aggregate_monthly_sales(reference: date, months_back: int = 6) -> Tuple[List[str], List[Decimal]]:
    month_bounds = [_month_bounds(reference, -offset) for offset in reversed(range(months_back))]
    labels = [start.strftime("%Y-%m") for start, _ in month_bounds]
    totals: List[Decimal] = []

    for start, end in month_bounds:
        total = (
            db.session.query(func.coalesce(func.sum(Invoice.gross_amount), 0))
            .filter(Invoice.issue_date >= start)
            .filter(Invoice.issue_date < end)
            .scalar()
        )
        totals.append(Decimal(total).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    return labels, totals


def _aggregate_ndg(reference: date, months_back: int = 6) -> Tuple[List[str], List[Decimal]]:
    month_bounds = [_month_bounds(reference, -offset) for offset in reversed(range(months_back))]
    labels = [start.strftime("%Y-%m") for start, _ in month_bounds]
    totals: List[Decimal] = []

    for start, end in month_bounds:
        ndg_total = (
            db.session.query(func.coalesce(func.sum(NDGDocument.amount), 0))
            .filter(NDGDocument.document_date >= start)
            .filter(NDGDocument.document_date < end)
            .scalar()
        )
        ndg_decimal = Decimal(str(ndg_total))
        combined = ndg_decimal + _sales_sum_between(start, end)
        totals.append(combined.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

    return labels, totals
def _month_bounds(reference: date, month_offset: int) -> Tuple[date, date]:
    target_year, target_month = _shift_month(reference.year, reference.month, month_offset)
    start = date(target_year, target_month, 1)
    next_year, next_month = _shift_month(target_year, target_month, 1)
    end = date(next_year, next_month, 1)
    return start, end


def _shift_month(year: int, month: int, delta: int) -> Tuple[int, int]:
    idx = (year * 12 + (month - 1)) + delta
    new_year = idx // 12
    new_month = idx % 12 + 1
    return new_year, new_month


def _current_month_ndg_usage(reference: date) -> Decimal:
    start, end = _month_bounds(reference, 0)
    total_docs = (
        db.session.query(func.coalesce(func.sum(NDGDocument.amount), 0))
        .filter(NDGDocument.document_date >= start)
        .filter(NDGDocument.document_date < end)
        .scalar()
    )
    ndg_decimal = Decimal(str(total_docs))
    combined = ndg_decimal + _sales_sum_between(start, end)
    return combined.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
def _month_name_pl(year: int, month: int, capitalize: bool = True) -> str:
    name = MONTH_NAMES_PL[month - 1]
    if capitalize:
        name = name.capitalize()
    return f"{name} {year}"


def _month_key_to_date(label: str) -> date:
    year, month = map(int, label.split("-"))
    return date(year, month, 1)


def _next_document_number(document_type: str, issue_date: date) -> str:
    start, end = _month_bounds(issue_date, 0)
    numbers = (
        db.session.query(Invoice.number)
        .filter(Invoice.document_type == document_type)
        .filter(Invoice.issue_date >= start)
        .filter(Invoice.issue_date < end)
        .all()
    )
    highest = 0
    for (number,) in numbers:
        if not number:
            continue
        parts = number.split("/")
        if not parts:
            continue
        try:
            value = int(parts[0])
        except (ValueError, TypeError):
            continue
        highest = max(highest, value)
    next_number = highest + 1
    return f"{next_number}/{issue_date.month}/{issue_date.year}"


def _pdf_with_title(title: str | None = None) -> Tuple[FPDF, str]:
    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.add_page()
    font_family = "Helvetica"

    if PDF_FONT_PATH and PDF_FONT_PATH.exists():
        try:
            font_path = str(PDF_FONT_PATH)
            pdf.add_font("DocumentFont", "", font_path)
            pdf.add_font("DocumentFont", "B", font_path)
            font_family = "DocumentFont"
        except Exception:
            app.logger.warning("Nie udało się załadować czcionki %s.", PDF_FONT_PATH)

    title_text = (title or "").strip()
    if title_text:
        pdf.set_font(font_family, size=16)
        pdf.cell(0, 10, title_text, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
        pdf.ln(4)
    pdf.set_font(font_family, size=11)
    return pdf, font_family


def _format_currency(value: Decimal | float | int) -> str:
    if not isinstance(value, Decimal):
        value = Decimal(str(value))
    value = value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    formatted = f"{value:,.2f}".replace(",", " ").replace(".", ",")
    return f"{formatted} zł"


def _pdf_output(pdf: FPDF) -> bytes:
    data = pdf.output(dest="S")
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, bytes):
        return data
    try:
        return data.encode("latin-1")
    except UnicodeEncodeError:
        return data.encode("latin-1", "ignore")


_UNITS = [
    ["zero", "jeden", "dwa", "trzy", "cztery", "pięć", "sześć", "siedem", "osiem", "dziewięć"],
    ["", "jeden", "dwa", "trzy", "cztery", "pięć", "sześć", "siedem", "osiem", "dziewięć"],
]
_TEENS = ["dziesięć", "jedenaście", "dwanaście", "trzynaście", "czternaście", "piętnaście", "szesnaście", "siedemnaście", "osiemnaście", "dziewiętnaście"]
_TENS = ["", "", "dwadzieścia", "trzydzieści", "czterdzieści", "pięćdziesiąt", "sześćdziesiąt", "siedemdziesiąt", "osiemdziesiąt", "dziewięćdziesiąt"]
_HUNDREDS = ["", "sto", "dwieście", "trzysta", "czterysta", "pięćset", "sześćset", "siedemset", "osiemset", "dziewięćset"]
_GROUPS = [
    ("złoty", "złote", "złotych"),
    ("tysiąc", "tysiące", "tysięcy"),
    ("milion", "miliony", "milionów"),
    ("miliard", "miliardy", "miliardów"),
]


def _declension(number: int, forms: Tuple[str, str, str]) -> str:
    if number == 1:
        return forms[0]
    if 2 <= number % 10 <= 4 and not (12 <= number % 100 <= 14):
        return forms[1]
    return forms[2]


def _invoice_pdf_bytes(invoice: Invoice, items: Sequence[dict]) -> bytes:
    document_type = (invoice.document_type or "").lower()
    pdf, font = _pdf_with_title(None)
    if document_type == "paragon":
        _render_paragon_document(pdf, font, invoice, items)
    else:
        _render_invoice_document(pdf, font, invoice, items)
    return _pdf_output(pdf)


def _render_invoice_document(pdf: FPDF, font: str, invoice: Invoice, items: Sequence[dict]) -> None:
    sale_label = (
        invoice.sale_date.strftime("%Y-%m-%d")
        if invoice.sale_date
        else invoice.issue_date.strftime("%Y-%m-%d")
    )
    doc_label = (invoice.document_type or "Dokument").strip().capitalize() or "Dokument"
    number_label = (invoice.number or "").strip() or "-"

    pdf.set_font(font, size=18)
    pdf.cell(0, 10, doc_label, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font(font, size=12)
    pdf.cell(0, 7, number_label, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(4)

    meta_rows = [
        ("Miejsce wystawienia", invoice.issue_place or DEFAULT_ISSUE_PLACE),
        ("Data wystawienia", invoice.issue_date.strftime("%Y-%m-%d")),
        ("Data sprzedaży", sale_label),
    ]
    meta_width = 82
    gap = 8
    block_top_y = pdf.get_y()
    buyer_width = max(90, pdf.w - pdf.l_margin - pdf.r_margin - meta_width - gap)

    pdf.set_xy(pdf.l_margin, block_top_y)
    pdf.set_fill_color(235, 235, 235)
    pdf.set_font(font, style="B", size=10)
    pdf.multi_cell(buyer_width, 7, "Nabywca", border=1, fill=True)
    pdf.set_font(font, size=10)
    client_name = (invoice.client_name or "Klient detaliczny").strip() or "Klient detaliczny"
    nip_value = f"NIP: {invoice.client_tax_id}" if invoice.client_tax_id else "NIP: -"
    address_lines = [line.strip() for line in (invoice.client_address or "").splitlines() if line.strip()] or ["-"]
    buyer_lines = [client_name, nip_value, *address_lines]
    pdf.set_x(pdf.l_margin)
    pdf.multi_cell(buyer_width, 5.2, "\n".join(buyer_lines), border=1, align="L")
    buyer_bottom = pdf.get_y()

    meta_x = pdf.w - pdf.r_margin - meta_width
    pdf.set_xy(meta_x, block_top_y)
    pdf.set_fill_color(241, 241, 241)
    for label, value in meta_rows:
        pdf.set_xy(meta_x, pdf.get_y())
        pdf.set_font(font, style="B", size=9)
        pdf.multi_cell(meta_width, 6, label, border=1, fill=True)
        pdf.set_xy(meta_x, pdf.get_y())
        pdf.set_font(font, size=10)
        pdf.multi_cell(meta_width, 7, value or "-", border=1)
    meta_bottom = pdf.get_y()
    pdf.set_y(max(buyer_bottom, meta_bottom) + 8)

    pdf.set_font(font, size=10)
    pdf.cell(0, 7, "Sprzedawca", fill=True, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font(font, size=12)
    pdf.cell(0, 7, SELLER.name, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font(font, size=10)
    for line in SELLER.address_lines:
        pdf.cell(0, 6, line, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    if SELLER.bank_account:
        pdf.cell(0, 6, f"Konto bankowe: {SELLER.bank_account}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    if SELLER.extra_info:
        for key, value in SELLER.extra_info.items():
            pdf.cell(0, 6, f"{key.capitalize()}: {value}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(4)

    headers = ["Lp.", "Nazwa pełna", "Ilość", "Jm", "Cena brutto", "Wartość brutto"]
    widths = [10, 88, 20, 12, 30, 30]
    _pdf_table_header(pdf, font, headers, widths, cell_height=9)

    if not items:
        pdf.cell(sum(widths), 7, "Brak pozycji", border=1, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    else:
        aligns = ["R", "L", "R", "L", "R", "R"]
        for idx, item in enumerate(items, 1):
            row = [
                str(idx),
                (item.get("description") or "").strip() or "-",
                _format_quantity(item.get("quantity", 0)),
                item.get("unit") or "",
                _format_currency(item.get("unit_price_gross", Decimal("0"))),
                _format_currency(item.get("line_total_gross", Decimal("0"))),
            ]
            _pdf_table_row(pdf, widths, row, aligns=aligns, line_height=8)

    pdf.set_font(font, style="B", size=10)
    pdf.set_fill_color(243, 246, 250)
    left_span = sum(widths[:-2])
    total_row_height = 9.5
    x_start = pdf.get_x()
    y_start = pdf.get_y()
    pdf.cell(left_span, total_row_height, "", border=1, fill=True)
    pdf.cell(widths[-2], total_row_height, "", border=1, fill=True)
    pdf.cell(widths[-1], total_row_height, "", border=1, fill=True)
    pdf.set_xy(x_start + left_span, y_start)
    pdf.cell(widths[-2], total_row_height, "Razem (PLN)", border=0, align="R")
    pdf.set_xy(x_start + left_span + widths[-2], y_start)
    pdf.cell(widths[-1], total_row_height, _format_currency(invoice.gross_amount), border=0, align="R")
    pdf.set_y(y_start + total_row_height)
    pdf.ln(4)

    amount_paid, remaining = _payment_breakdown(invoice)
    table_width = sum(widths)
    value_width = widths[-1] + widths[-2]
    label_width = table_width - value_width
    words = _number_to_words_pl(invoice.gross_amount)

    summary_rows = [
        (
            "DO ZAPŁATY",
            f"{_format_currency(invoice.gross_amount)}",
            {
                "fill": (232, 232, 232),
                "label_style": "B",
                "label_size": 10,
                "label_align": "C",
                "value_style": "B",
                "value_size": 14,
                "value_align": "C",
            },
        ),
        ("Słownie", words, {"label_style": "B"}),
        (
            "Sposób płatności",
            invoice.payment_method or "BLIK",
            {"label_style": "B"},
        ),
        (
            "Zapłacono",
            _format_currency(amount_paid),
            {"label_style": "B", "value_align": "R"},
        ),
        (
            "Pozostało do zapłaty",
            _format_currency(remaining),
            {"label_style": "B", "value_align": "R", "value_style": "B"},
        ),
    ]
    _draw_labeled_rows(pdf, font, label_width, value_width, summary_rows, line_height=5.8)
    pdf.ln(4)

    if invoice.notes:
        pdf.set_font(font, size=10)
        pdf.multi_cell(0, 6, f"Uwagi: {invoice.notes}")
        pdf.ln(4)

    signature_width = 72
    signature_height = 18
    signature_y = pdf.get_y()
    right_signature_x = pdf.w - pdf.r_margin - signature_width

    pdf.rect(pdf.l_margin, signature_y, signature_width, signature_height)
    pdf.set_xy(pdf.l_margin, signature_y + 4)
    pdf.set_font(font, style="B", size=10)
    pdf.cell(signature_width, 6, "Jakub Lis", align="C")
    pdf.set_xy(pdf.l_margin, signature_y + 10)
    pdf.set_font(font, size=9)
    pdf.cell(signature_width, 5, "Wystawił(a)", align="C")

    pdf.rect(right_signature_x, signature_y, signature_width, signature_height)
    pdf.set_xy(right_signature_x, signature_y + 4)
    pdf.set_font(font, style="B", size=10)
    pdf.cell(signature_width, 6, "Odebrał(a)", align="C")
    pdf.set_xy(right_signature_x, signature_y + 10)
    pdf.set_font(font, size=9)
    pdf.cell(signature_width, 5, "czytelny podpis", align="C")

    pdf.set_y(signature_y + signature_height + 2)


def _render_paragon_document(pdf: FPDF, font: str, invoice: Invoice, items: Sequence[dict]) -> None:
    sale_label = (
        invoice.sale_date.strftime("%Y-%m-%d")
        if invoice.sale_date
        else invoice.issue_date.strftime("%Y-%m-%d")
    )

    number_label = (invoice.number or "").strip() or "-"
    match = re.match(r"(?i)paragon\s*(.*)", number_label)
    display_number = match.group(1).strip() if match and match.group(1).strip() else number_label

    pdf.set_font(font, size=18)
    pdf.cell(0, 10, "Paragon", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font(font, size=12)
    pdf.cell(0, 7, display_number, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(4)

    meta_rows = [
        ("Miejsce wystawienia", invoice.issue_place or DEFAULT_ISSUE_PLACE),
        ("Data wystawienia", invoice.issue_date.strftime("%Y-%m-%d")),
        ("Data sprzedaży", sale_label),
    ]
    meta_width = 82
    meta_x = pdf.w - pdf.r_margin - meta_width
    meta_top_y = pdf.get_y()
    pdf.set_fill_color(241, 241, 241)
    current_meta_y = meta_top_y
    for label, value in meta_rows:
        pdf.set_xy(meta_x, current_meta_y)
        pdf.set_font(font, style="B", size=9)
        pdf.multi_cell(meta_width, 6, label, border=1, fill=True)
        pdf.set_xy(meta_x, pdf.get_y())
        pdf.set_font(font, size=10)
        pdf.multi_cell(meta_width, 7, value or "-", border=1)
        current_meta_y = pdf.get_y()
    meta_bottom = current_meta_y

    pdf.set_xy(meta_x, meta_bottom + 2)
    pdf.set_fill_color(235, 235, 235)
    pdf.set_font(font, style="B", size=10)
    pdf.multi_cell(meta_width, 7, "Sprzedawca", border=1, fill=True, align="C")
    pdf.set_font(font, size=10)
    seller_lines = [SELLER.name, *SELLER.address_lines]
    if SELLER.bank_account:
        seller_lines.append(f"Konto bankowe: {SELLER.bank_account}")
    if SELLER.extra_info:
        for key, value in SELLER.extra_info.items():
            seller_lines.append(f"{key.capitalize()}: {value}")
    for line in seller_lines:
        pdf.set_xy(meta_x, pdf.get_y())
        pdf.multi_cell(meta_width, 6, line, border=1)
    seller_bottom = pdf.get_y()

    pdf.set_y(max(seller_bottom, meta_top_y) + 10)
    pdf.set_x(pdf.l_margin)

    headers = ["Lp.", "Nazwa pełna", "Ilość", "Jm", "Cena brutto", "Wartość brutto"]
    widths = [10, 88, 20, 12, 30, 30]
    _pdf_table_header(pdf, font, headers, widths, cell_height=9)

    if not items:
        pdf.cell(sum(widths), 7, "Brak pozycji", border=1, new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    else:
        aligns = ["R", "L", "R", "L", "R", "R"]
        for idx, item in enumerate(items, 1):
            row = [
                str(idx),
                (item.get("description") or "").strip() or "-",
                _format_quantity(item.get("quantity", 0)),
                item.get("unit") or "",
                _format_currency_plain(item.get("unit_price_gross", Decimal("0"))),
                _format_currency_plain(item.get("line_total_gross", Decimal("0"))),
            ]
            _pdf_table_row(pdf, widths, row, aligns=aligns, line_height=8.5)

    pdf.set_font(font, style="B", size=10)
    pdf.set_fill_color(243, 246, 250)
    left_span = sum(widths[:-2])
    total_row_height = 9.5
    x_start = pdf.get_x()
    y_start = pdf.get_y()
    pdf.cell(left_span, total_row_height, "", border=1, fill=True)
    pdf.cell(widths[-2], total_row_height, "", border=1, fill=True)
    pdf.cell(widths[-1], total_row_height, "", border=1, fill=True)
    pdf.set_xy(x_start + left_span, y_start)
    pdf.cell(widths[-2], total_row_height, "Razem (PLN)", border=0, align="R")
    pdf.set_xy(x_start + left_span + widths[-2], y_start)
    pdf.cell(widths[-1], total_row_height, _format_currency_plain(invoice.gross_amount), border=0, align="R")
    pdf.set_y(y_start + total_row_height)
    pdf.ln(4)

    amount_paid, remaining = _payment_breakdown(invoice)
    table_width = sum(widths)
    value_width = widths[-1] + widths[-2]
    label_width = table_width - value_width
    words = _number_to_words_pl(invoice.gross_amount)

    summary_rows = [
        (
            "DO ZAPŁATY",
            f"{_format_currency_plain(invoice.gross_amount)} PLN",
            {
                "fill": (232, 232, 232),
                "label_style": "B",
                "label_size": 10,
                "label_align": "C",
                "value_style": "B",
                "value_size": 14,
                "value_align": "C",
            },
        ),
        ("Słownie", words, {"label_style": "B"}),
        (
            "Sposób płatności",
            invoice.payment_method or "Inny",
            {"label_style": "B"},
        ),
        (
            "Zapłacono",
            f"{_format_currency_plain(amount_paid)} PLN",
            {"label_style": "B", "value_align": "R"},
        ),
        (
            "Pozostało do zapłaty",
            f"{_format_currency_plain(remaining)} PLN",
            {"label_style": "B", "value_align": "R", "value_style": "B"},
        ),
    ]
    _draw_labeled_rows(pdf, font, label_width, value_width, summary_rows, line_height=5.8)
    pdf.ln(4)

    if invoice.notes:
        pdf.set_font(font, size=10)
        pdf.multi_cell(0, 6, f"Uwagi: {invoice.notes}")
        pdf.ln(4)

    signature_width = 72
    signature_height = 18
    signature_y = pdf.get_y()
    right_signature_x = pdf.w - pdf.r_margin - signature_width

    pdf.rect(pdf.l_margin, signature_y, signature_width, signature_height)
    pdf.set_xy(pdf.l_margin, signature_y + 4)
    pdf.set_font(font, style="B", size=10)
    pdf.cell(signature_width, 6, "Jakub Lis", align="C")
    pdf.set_xy(pdf.l_margin, signature_y + 10)
    pdf.set_font(font, size=9)
    pdf.cell(signature_width, 5, "Wystawił(a)", align="C")

    pdf.rect(right_signature_x, signature_y, signature_width, signature_height)
    pdf.set_xy(right_signature_x, signature_y + 4)
    pdf.set_font(font, style="B", size=10)
    pdf.cell(signature_width, 6, "Odebrał(a)", align="C")
    pdf.set_xy(right_signature_x, signature_y + 10)
    pdf.set_font(font, size=9)
    pdf.cell(signature_width, 5, "czytelny podpis", align="C")

    pdf.set_y(signature_y + signature_height + 2)


def _draw_labeled_rows(
    pdf: FPDF,
    font: str,
    label_width: float,
    value_width: float,
    rows: Sequence[Tuple[str, str, dict | None]],
    line_height: float = 6.0,
) -> None:
    label_width = max(20.0, label_width)
    value_width = max(20.0, value_width)
    row_start_x = pdf.l_margin

    for label, value, options in rows:
        opts = options or {}
        padding_x = opts.get("padding_x", 1.4)
        padding_y = opts.get("padding_y", 1.2)
        label_style = opts.get("label_style", "")
        label_size = opts.get("label_size", 9)
        value_style = opts.get("value_style", "")
        value_size = opts.get("value_size", 10)
        label_align = opts.get("label_align", "L")
        value_align = opts.get("value_align", "L")
        fill = opts.get("fill")

        label_box_width = max(4.0, label_width - 2 * padding_x)
        value_box_width = max(4.0, value_width - 2 * padding_x)

        pdf.set_font(font, style=label_style, size=label_size)
        label_lines = pdf.multi_cell(
            label_box_width, line_height, (label or "-"), split_only=True
        )
        pdf.set_font(font, style=value_style, size=value_size)
        value_lines = pdf.multi_cell(
            value_box_width, line_height, (value or "-"), split_only=True
        )
        label_count = max(1, len(label_lines))
        value_count = max(1, len(value_lines))
        content_height = line_height * max(label_count, value_count)
        row_height = content_height + 2 * padding_y
        row_y = pdf.get_y()

        if fill:
            pdf.set_fill_color(*fill)
            pdf.rect(row_start_x, row_y, label_width + value_width, row_height, style="F")

        pdf.rect(row_start_x, row_y, label_width, row_height)
        pdf.rect(row_start_x + label_width, row_y, value_width, row_height)

        pdf.set_xy(row_start_x + padding_x, row_y + padding_y)
        pdf.set_font(font, style=label_style, size=label_size)
        pdf.multi_cell(
            label_box_width, line_height, (label or "-"), border=0, align=label_align
        )

        pdf.set_xy(row_start_x + label_width + padding_x, row_y + padding_y)
        pdf.set_font(font, style=value_style, size=value_size)
        pdf.multi_cell(
            value_box_width, line_height, (value or "-"), border=0, align=value_align
        )

        pdf.set_y(row_y + row_height)


def _pdf_table_header(
    pdf: FPDF, font: str, headers: Sequence[str], widths: Sequence[float], cell_height: float = 7.0
) -> None:
    pdf.set_font(font, style="B", size=10)
    pdf.set_fill_color(243, 246, 250)
    for header, width in zip(headers, widths):
        pdf.cell(width, cell_height, header, border=1, fill=True)
    pdf.ln()


def _document_display_title(invoice: Invoice) -> str:
    doc_type = (invoice.document_type or "Dokument").strip()
    number = (invoice.number or "").strip()
    normalized_doc = doc_type.lower()
    normalized_number = number.lower()

    if number and normalized_number.startswith(normalized_doc):
        return number
    if doc_type and number:
        return f"{doc_type.capitalize()} {number}"
    return (doc_type or number or "Dokument").strip()


def _payment_breakdown(invoice: Invoice) -> Tuple[Decimal, Decimal]:
    amount_paid = Decimal(invoice.amount_paid) if invoice.amount_paid is not None else Decimal("0")
    remaining = max(invoice.gross_amount - amount_paid, Decimal("0"))
    return amount_paid, remaining

def _sales_register_pdf_bytes(invoices: Sequence[Invoice]) -> bytes:
    pdf, font = _pdf_with_title("Ewidencja sprzedaży")

    if not invoices:
        pdf.multi_cell(0, 6, "Brak dokumentów sprzedażowych w bazie.")
        return _pdf_output(pdf)

    headers = ["Data", "Numer", "Kontrahent", "Netto", "VAT", "Brutto"]
    widths = [28, 40, 58, 24, 22, 28]

    pdf.set_font(font, size=10)
    for header, width in zip(headers, widths):
        pdf.cell(width, 7, header, border=1)
    pdf.ln()

    total_net = Decimal("0")
    total_tax = Decimal("0")
    total_gross = Decimal("0")

    for invoice in invoices:
        tax_amount = invoice.gross_amount - invoice.net_amount
        total_net += invoice.net_amount
        total_tax += tax_amount
        total_gross += invoice.gross_amount

        cells = [
            invoice.issue_date.strftime("%Y-%m-%d"),
            invoice.number,
            invoice.client_name,
            _format_currency(invoice.net_amount),
            _format_currency(tax_amount),
            _format_currency(invoice.gross_amount),
        ]
        for value, width in zip(cells, widths):
            trimmed = value
            if len(trimmed) > 30:
                trimmed = trimmed[:27] + "..."
            pdf.cell(width, 7, trimmed, border=1)
        pdf.ln()

    pdf.ln(4)
    pdf.set_font(font, size=11)
    pdf.cell(0, 6, f"Suma netto: {_format_currency(total_net)}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 6, f"Suma VAT: {_format_currency(total_tax)}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.cell(0, 6, f"Suma brutto: {_format_currency(total_gross)}", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    return _pdf_output(pdf)


def _ndg_register_pdf_bytes(documents: Sequence[NDGDocument]) -> bytes:
    pdf, font = _pdf_with_title("Dokumenty NDG")

    if not documents:
        pdf.multi_cell(0, 6, "Brak zapisanych dokumentów kosztowych NDG.")
        return _pdf_output(pdf)

    headers = ["Data", "Numer", "Dostawca", "Kwota", "Uwagi"]
    widths = [25, 35, 60, 25, 45]

    pdf.set_font(font, size=10)
    for header, width in zip(headers, widths):
        pdf.cell(width, 7, header, border=1)
    pdf.ln()

    total_amount = Decimal("0")

    for doc in documents:
        total_amount += doc.amount
        values = [
            doc.document_date.strftime("%Y-%m-%d"),
            doc.number,
            doc.supplier_name,
            _format_currency(doc.amount),
            (doc.description or "")[:45] + ("..." if doc.description and len(doc.description) > 45 else ""),
        ]
        for value, width in zip(values, widths):
            trimmed = value if len(value) <= 40 else value[:37] + "..."
            pdf.cell(width, 7, trimmed, border=1)
        pdf.ln()

    pdf.ln(4)
    pdf.set_font(font, size=11)
    pdf.multi_cell(0, 6, f"Suma kosztów NDG: {_format_currency(total_amount)}")

    return _pdf_output(pdf)


if __name__ == "__main__":
    app.run(debug=True)
