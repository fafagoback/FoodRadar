# FoodRadar Cloudflare Worker check

The Worker is isolated from the current GitHub Pages site. The workflow probes Cloudflare account credentials and Turso read-only credentials, without revealing their values. With valid credentials, it deploys a separate Worker and stores the Turso connection as Worker secrets.

Endpoints: `GET /api/health` checks Turso; `GET /api/stores?limit=50&cursor=0` demonstrates indexed, database-side cursor paging.

This is a connectivity proof, **not** the final indexed product search migration. Do not route production traffic until product search is built and tested.
