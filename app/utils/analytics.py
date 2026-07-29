import logging
import os
import matplotlib.pyplot as plt
import numpy as np
from app.config import CHART_PATH

logger = logging.getLogger(__name__)

def generate_analytics_and_chart(company_scores: list) -> dict:
    """
    Computes NumPy stats and generates a Matplotlib visualization of company risks.
    """
    if not company_scores:
        logger.warning("No company scores provided for analytics.")
        return {}

    # Extract risk scores
    scores = [c["risk_score"] for c in company_scores]
    scores_arr = np.array(scores)

    # 1. Compute statistics
    mean_val = float(np.mean(scores_arr))
    median_val = float(np.median(scores_arr))
    std_val = float(np.std(scores_arr))
    pct_90_val = float(np.percentile(scores_arr, 90))

    stats = {
        "mean": round(mean_val, 2),
        "median": round(median_val, 2),
        "std_dev": round(std_val, 2),
        "percentile_90": round(pct_90_val, 2)
    }

    # 2. Sort companies to get top 10
    sorted_companies = sorted(company_scores, key=lambda x: x["risk_score"], reverse=True)
    top_10 = sorted_companies[:10]

    company_names = [c["company_name"] for c in top_10]
    company_scores_top_10 = [c["risk_score"] for c in top_10]
    
    # Reverse order so highest is at the top in horizontal bar chart
    company_names.reverse()
    company_scores_top_10.reverse()

    # 3. Create chart
    plt.figure(figsize=(10, 6))
    
    # Modern professional colors: viridis gradient
    colors = plt.cm.viridis(np.linspace(0.4, 0.8, len(top_10)))
    
    y_pos = np.arange(len(company_names))
    bars = plt.barh(y_pos, company_scores_top_10, color=colors, edgecolor='none', height=0.6)
    plt.yticks(y_pos, company_names)
    
    # Labels, title, and styling
    plt.title("Top 10 Companies by Risk Score", fontsize=14, fontweight='bold', pad=15)
    plt.xlabel("Risk Score (0 - 100)", fontsize=11, labelpad=10)
    plt.ylabel("Company Name", fontsize=11, labelpad=10)
    plt.xlim(0, 105)
    
    # Add grid lines
    plt.grid(axis='x', linestyle='--', alpha=0.5)
    
    # Add values on the bars
    for bar in bars:
        width = bar.get_width()
        plt.text(
            width + 1.5,
            bar.get_y() + bar.get_height() / 2,
            f"{width:.2f}",
            ha='left',
            va='center',
            fontsize=10,
            fontweight='bold',
            color='#333333'
        )

    # Tight layout to avoid cutting off labels
    plt.tight_layout()

    # Ensure output folder exists
    os.makedirs(os.path.dirname(CHART_PATH), exist_ok=True)
    
    # Save chart
    plt.savefig(CHART_PATH, dpi=150)
    plt.close()

    logger.info(f"Analytics chart successfully saved to {CHART_PATH}")
    return stats
