"""Public subscription prices shared by the pricing page and checkout guard."""

PRO_PRICE_USD = {"month": 12, "year": 120}

PRO_PRICE_CENTS = {interval: amount * 100 for interval, amount in PRO_PRICE_USD.items()}
