# NEW FILE - completely independent
# No imports from existing services
class CreatorMetricsCalculator:
    @staticmethod
    def calculate_engagement_score(df):
        if df.is_empty():
            return 0.0
        return min(float(df["Engagement Rate Raw"].mean() * 100), 100.0)

    # ... other methods (see IMPLEMENTATION_ROADMAP.md)
