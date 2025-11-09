(() => {
    window.addEventListener("DOMContentLoaded", () => {
        const bootstrap = window.dashboardBootstrap || {};
        const hasChartJs = typeof window.Chart !== "undefined";
        const usageCanvas = document.getElementById("usageChart");
        const annualCanvas = document.getElementById("annualSalesChart");
        const healthToggle = document.querySelector("[data-health-toggle]");
        const healthPanel = document.getElementById("companyHealthPanel");

        const currencyFormatter = new Intl.NumberFormat("pl-PL", {
            style: "currency",
            currency: "PLN",
            minimumFractionDigits: 2,
        });

        const colors = {
            used: "#f78a1d",
            remaining: "#1f9dd1",
            background: "#ffffff",
            bar: "#3f84d6",
        };

        let usageChart = null;
        let annualChart = null;

        if (usageCanvas && hasChartJs) {
            const donutPayload = normalizeDonut(bootstrap);
            updateUsageChart(donutPayload);
            refreshFromApi();
        }

        setupCompanyHealth();

        function setupCompanyHealth() {
            if (!healthToggle || !healthPanel) {
                return;
            }
            healthToggle.addEventListener("click", () => {
                const isHidden = healthPanel.hasAttribute("hidden");
                if (isHidden) {
                    healthPanel.removeAttribute("hidden");
                    healthToggle.classList.add("is-open");
                    healthToggle.setAttribute("aria-expanded", "true");
                    if (hasChartJs) {
                        requestAnimationFrame(() => buildAnnualChart());
                    }
                } else {
                    healthPanel.setAttribute("hidden", "");
                    healthToggle.classList.remove("is-open");
                    healthToggle.setAttribute("aria-expanded", "false");
                }
            });
        }

        function buildAnnualChart() {
            if (!annualCanvas || annualChart || !hasChartJs) {
                return;
            }
            const dataset = normalizeAnnual(bootstrap.annual || {});
            if (!dataset.labels.length) {
                return;
            }
            annualChart = new Chart(annualCanvas, {
                type: "bar",
                data: {
                    labels: dataset.labels,
                    datasets: [
                        {
                            label: "Sprzedaż miesięczna",
                            data: dataset.series,
                            backgroundColor: "rgba(63, 132, 214, 0.25)",
                            borderColor: colors.bar,
                            borderWidth: 2,
                            borderRadius: 6,
                            hoverBackgroundColor: "rgba(63, 132, 214, 0.45)",
                        },
                    ],
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: {
                            beginAtZero: true,
                            ticks: {
                                callback(value) {
                                    return currencyFormatter.format(value);
                                },
                            },
                        },
                    },
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            callbacks: {
                                label(context) {
                                    const value = currencyFormatter.format(context.parsed.y ?? context.parsed ?? 0);
                                    return `Sprzedaż: ${value}`;
                                },
                            },
                        },
                    },
                },
            });
        }

        function refreshFromApi() {
            fetch("/api/dashboard-data", { headers: { Accept: "application/json" } })
                .then((response) => (response.ok ? response.json() : null))
                .then((payload) => {
                    if (!payload) {
                        return;
                    }
                    const dataset = normalizeDonut(payload);
                    updateUsageChart(dataset);
                })
                .catch((error) => {
                    console.warn("Nie udało się odświeżyć danych dashboardu.", error);
                });
        }

        function normalizeDonut(payload) {
            const base = Array.isArray(payload.donut) ? payload.donut.slice(0, 2) : [];
            if (base.length >= 2) {
                return {
                    donut: base.map((value) => Math.max(Number(value) || 0, 0)),
                };
            }

            const usage = Number(payload.current_ndg_usage);
            const remaining = Number(payload.current_ndg_remaining);
            if (Number.isFinite(usage) && Number.isFinite(remaining)) {
                return {
                    donut: [Math.max(usage, 0), Math.max(remaining, 0)],
                };
            }

            const ndgLimit = Number(payload.ndg_limit);
            if (Number.isFinite(ndgLimit) && Number.isFinite(remaining)) {
                const parsedRemaining = Math.max(remaining, 0);
                const parsedUsage = Math.max(ndgLimit - parsedRemaining, 0);
                return {
                    donut: [parsedUsage, parsedRemaining],
                };
            }

            return { donut: [0, 0] };
        }

        function normalizeAnnual(payload) {
            const labels = Array.isArray(payload.labels) ? payload.labels : [];
            const series = Array.isArray(payload.series)
                ? payload.series.map((value) => Number(value) || 0)
                : [];
            return { labels, series };
        }

        function updateUsageChart(data) {
            if (!usageCanvas || !hasChartJs) {
                return;
            }
            const dataset = data.donut;
            if (!usageChart) {
                usageChart = new Chart(usageCanvas, {
                    type: "doughnut",
                    data: {
                        labels: ["Wykorzystany limit", "Pozostały limit"],
                        datasets: [
                            {
                                data: dataset,
                                backgroundColor: [colors.used, colors.remaining],
                                borderWidth: 2,
                                borderColor: colors.background,
                                hoverOffset: 4,
                            },
                        ],
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        cutout: "70%",
                        plugins: {
                            legend: { display: false },
                            tooltip: {
                                callbacks: {
                                    label(context) {
                                        const label = context.label || "";
                                        const value = currencyFormatter.format(context.parsed ?? 0);
                                        return `${label}: ${value}`;
                                    },
                                },
                            },
                        },
                    },
                });
                return;
            }

            usageChart.data.datasets[0].data = dataset;
            usageChart.update();
        }
    });
})();
