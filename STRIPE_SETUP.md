# Stripe Setup Guide

This guide explains how to set up Stripe integration for ViralVibes billing.

## Quick Start

1. **Copy the example environment file:**
   ```bash
   cp .env.example .env
   ```

2. **Fill in Stripe credentials** in your `.env` file (see below for where to get them)

3. **Restart the application**

## Required Environment Variables

### API Keys
Get these from [Stripe Dashboard](https://dashboard.stripe.com/apikeys):

- `STRIPE_SECRET_KEY` — Your secret API key (starts with `sk_test_` or `sk_live_`)
- `STRIPE_PUBLISHABLE_KEY` — Your publishable key (starts with `pk_test_` or `pk_live_`)
- `STRIPE_WEBHOOK_SECRET` — Webhook signing secret for incoming webhooks (starts with `whsec_`)

### Price IDs
Create prices in your Stripe Dashboard at:
**https://dashboard.stripe.com/products** → Select/Create Product → **Pricing**

You need to create **4 prices**:

1. **Pro — Monthly**
   - Save the Price ID to `STRIPE_PRICE_PRO_MONTHLY`
   - Example: `price_1234567890abcdef`

2. **Pro — Annual**
   - Save the Price ID to `STRIPE_PRICE_PRO_ANNUAL`
   - Example: `price_0987654321fedcba`

3. **Agency — Monthly**
   - Save the Price ID to `STRIPE_PRICE_AGENCY_MONTHLY`
   - Example: `price_abcdefghijklmnop`

4. **Agency — Annual**
   - Save the Price ID to `STRIPE_PRICE_AGENCY_ANNUAL`
   - Example: `price_ponmlkjihgfedcba`

### Example .env

```
STRIPE_SECRET_KEY=sk_test_your_secret_key_here
STRIPE_PUBLISHABLE_KEY=pk_test_your_publishable_key_here
STRIPE_WEBHOOK_SECRET=whsec_your_webhook_secret_here

STRIPE_PRICE_PRO_MONTHLY=price_1234567890abcdef
STRIPE_PRICE_PRO_ANNUAL=price_0987654321fedcba
STRIPE_PRICE_AGENCY_MONTHLY=price_abcdefghijklmnop
STRIPE_PRICE_AGENCY_ANNUAL=price_ponmlkjihgfedcba
```

## Webhook Setup (Production)

For production deployments, configure Stripe webhooks:

1. Go to **Settings** → **Webhooks** in Stripe Dashboard
2. Add new endpoint pointing to: `https://yourdomain.com/billing/webhook`
3. Select these events:
   - `customer.subscription.created`
   - `customer.subscription.updated`
   - `customer.subscription.deleted`
   - `invoice.payment_succeeded`
4. Copy the signing secret to `STRIPE_WEBHOOK_SECRET`

## Troubleshooting

**Error: "You did not provide an API key"**
- `STRIPE_SECRET_KEY` is missing or empty
- Check `.env` file has the correct value
- Restart the application

**Error: "price→plan mapping will be incomplete"**
- One or more of the `STRIPE_PRICE_*` variables are missing
- Create them in Stripe Dashboard and add to `.env`
- Restart the application

**Checkout failing silently**
- Check application logs for Stripe API errors
- Ensure all 4 price IDs are configured
- Verify prices are part of an existing product in Stripe

## Testing

Use these test card numbers in Stripe test mode:

- Success: `4242 4242 4242 4242`
- Decline: `4000 0000 0000 0002`
- Always authenticate: `4000 2500 0000 0002`

Use any future date for expiry and any 3-digit CVC.
