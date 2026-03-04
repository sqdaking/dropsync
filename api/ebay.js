// DropSync — eBay Backend (Vercel Serverless)
// Improved Amazon variation, price, and image scraping

const EBAY_API  = 'https://api.ebay.com';
const EBAY_AUTH = 'https://auth.ebay.com';

module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const CLIENT_ID     = process.env.EBAY_CLIENT_ID;
  const CLIENT_SECRET = process.env.EBAY_CLIENT_SECRET;
  const RUNAME        = process.env.EBAY_RUNAME;
  const FRONTEND_URL  = process.env.FRONTEND_URL || '';
  const basicAuth     = 'Basic ' + Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64');
  const { action }    = req.query;

  let body = {};
  if (req.method === 'POST') {
    try { body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {}; } catch {}
  }

  try {

    if (action === 'auth') {
      const scopes = [
        'https://api.ebay.com/oauth/api_scope',
        'https://api.ebay.com/oauth/api_scope/sell.inventory',
        'https://api.ebay.com/oauth/api_scope/sell.inventory.readonly',
        'https://api.ebay.com/oauth/api_scope/sell.fulfillment',
        'https://api.ebay.com/oauth/api_scope/sell.fulfillment.readonly',
      ].join(' ');
      return res.redirect(`${EBAY_AUTH}/oauth2/authorize?client_id=${CLIENT_ID}&redirect_uri=${encodeURIComponent(RUNAME)}&response_type=code&scope=${encodeURIComponent(scopes)}`);
    }

    if (action === 'callback') {
      const code = req.query.code;
      if (!code) return res.status(400).send('<h2>Error: No code from eBay</h2>');
      const r = await fetch(`${EBAY_API}/identity/v1/oauth2/token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', Authorization: basicAuth },
        body: new URLSearchParams({ grant_type: 'authorization_code', code, redirect_uri: RUNAME }),
      });
      const d = await r.json();
      if (!d.access_token) return res.status(400).send(`<h2>Token error: ${JSON.stringify(d)}</h2>`);
      // Send token to parent window via postMessage (works with local file:// HTML)
      return res.send(`<!DOCTYPE html><html><head><title>eBay Connected</title>
<style>body{font-family:sans-serif;background:#0a0a0f;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh;margin:0;flex-direction:column;gap:16px}
.icon{font-size:48px}.title{font-size:22px;font-weight:700;color:#26de81}.sub{font-size:13px;color:#888}</style></head>
<body>
<div class="icon">✅</div>
<div class="title">eBay Connected!</div>
<div class="sub">This window will close automatically...</div>
<script>
  const token = ${JSON.stringify({
    type: 'ebay_token',
    access_token: d.access_token,
    refresh_token: d.refresh_token || '',
    expires_in: d.expires_in || 7200
  })};
  // Post to opener (popup flow)
  if (window.opener) {
    window.opener.postMessage(token, '*');
    setTimeout(() => window.close(), 1500);
  } else {
    // Fallback: redirect with params if opened as full page
    const p = new URLSearchParams({
      access_token: token.access_token,
      refresh_token: token.refresh_token,
      expires_in: token.expires_in
    });
    const dest = ${JSON.stringify(FRONTEND_URL || '')};
    if (dest) window.location.href = dest + '?' + p.toString();
    else document.querySelector('.sub').textContent = 'Copy your token: ' + token.access_token.slice(0,40) + '...';
  }
<\/script>
</body></html>`);
    }

    if (action === 'refresh') {
      const refresh_token = body.refresh_token || req.query.refresh_token;
      const r = await fetch(`${EBAY_API}/identity/v1/oauth2/token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', Authorization: basicAuth },
        body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token }),
      });
      return res.json(await r.json());
    }

    // ═══════════════════════════════════════════════════════════
    // SCRAPE — full variation + image + price extraction
    // ═══════════════════════════════════════════════════════════
    // Snippet action — dumps raw HTML around key Amazon data blocks
    if (action === 'snippet') {
      let url = req.query.url || body.url;
      if (!url) return res.json({ error: 'No URL' });
      if (url.includes('amazon.com')) {
        const m = url.match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})/);
        if (m) url = `https://www.amazon.com/dp/${m[1]}`;
      }
      const ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36';
      const r = await fetch(url, { headers: { 'User-Agent': ua, 'Accept-Language': 'en-US,en;q=0.9', 'Accept': 'text/html' } });
      const html = await r.text();
      const snippets = {};
      // Find 500 chars around each key term
      for (const key of ['asinVariationValues','asinToDimension','colorImages','priceToAsinList','unavailableAsinSet','inStockAsinSet','variationValues','twister-js-init','dimensionValuesData']) {
        const idx = html.indexOf(key);
        if (idx >= 0) snippets[key] = html.slice(Math.max(0,idx-20), idx+500);
        else snippets[key] = 'NOT FOUND';
      }
      return res.json({ snippets, htmlLength: html.length });
    }

    // Debug action — returns raw extracted product data including _debug info
    if (action === 'debug') {
      const url = req.query.url || body.url;
      if (!url) return res.json({ error: 'No URL provided. Add ?url=https://amazon.com/dp/...' });
      // Reuse scrape action
      req.query.action = 'scrape';
      // Fall through to scrape below — handled by returning full product with _debug
    }

    if (action === 'scrape' || action === 'debug') {
      let url = body.url || req.query.url;
      if (!url) return res.status(400).json({ error: 'No URL' });

      // ── Normalize any Amazon URL to clean dp/ASIN page ──────────────
      if (url.includes('amazon.com')) {
        // Extract ASIN from any Amazon URL format:
        // - /dp/B0XXXXXX
        // - /gp/product/B0XXXXXX
        // - /sspa/click?...dp/B0XXXXXX
        // - ?asin=B0XXXXXX
        // - sr_1_2...B0XXXXXX in path
        let asin = null;

        // Try /dp/ASIN or /gp/product/ASIN
        const dpMatch = url.match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})/);
        if (dpMatch) asin = dpMatch[1];

        // Try asin= query param
        if (!asin) {
          const asinParam = url.match(/[?&]asin=([A-Z0-9]{10})/i);
          if (asinParam) asin = asinParam[1];
        }

        // Try any 10-char ASIN-like string in the URL
        if (!asin) {
          const anyAsin = url.match(/\/([A-Z0-9]{10})(?:\/|\?|$)/);
          if (anyAsin) asin = anyAsin[1];
        }

        if (asin) {
          url = `https://www.amazon.com/dp/${asin}`;
          console.log('Normalized Amazon URL to:', url);
        }
      }

      // Normalize Walmart URLs too
      if (url.includes('walmart.com')) {
        const wmMatch = url.match(/\/ip\/(?:[^/]+\/)?(\d{6,})/);
        if (wmMatch) url = `https://www.walmart.com/ip/${wmMatch[1]}`;
      }

      const product = {
        url, source: '', title: '', price: '', images: [],
        description: '', brand: '', aspects: {},
        variations: [], variationImages: {}, hasVariations: false,
      };

      try {
        // Rotate user agents to avoid Amazon bot detection
        const USER_AGENTS = [
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
          'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0',
          'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3.1 Safari/605.1.15',
          'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        ];
        const ua = USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];

        // Try fetching with retries
        let html = '';
        let lastErr = null;
        for (let attempt = 0; attempt < 3; attempt++) {
          try {
            if (attempt > 0) await new Promise(r => setTimeout(r, 1500 * attempt));
            const htmlRes = await fetch(url, {
              headers: {
                'User-Agent': ua,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'Cache-Control': 'no-cache',
                'Pragma': 'no-cache',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Sec-Fetch-User': '?1',
                'Upgrade-Insecure-Requests': '1',
                'DNT': '1',
              },
              redirect: 'follow',
            });
            if (htmlRes.status === 503 || htmlRes.status === 429) {
              lastErr = `HTTP ${htmlRes.status} — Amazon rate limited`;
              continue;
            }
            html = await htmlRes.text();
            // Check if Amazon served a CAPTCHA page
            if (html.includes('Type the characters you see') || html.includes('Enter the characters you see') || html.includes('robot check')) {
              lastErr = 'Amazon CAPTCHA detected — try again in a few minutes';
              html = '';
              continue;
            }
            break; // success
          } catch(e) { lastErr = e.message; }
        }
        if (!html) return res.json({ success: false, error: lastErr || 'Failed to fetch page', product });

        // ── AMAZON ──────────────────────────────────────────────────────
        if (url.includes('amazon.com')) {
          product.source = 'amazon';

          // ── Title
          const titleM = html.match(/id="productTitle"[^>]*>\s*([\s\S]*?)\s*<\/span>/);
          if (titleM) product.title = titleM[1].replace(/<[^>]+>/g,'').trim().replace(/\s+/g,' ');

          // ── Brand
          const brandM = html.match(/id="bylineInfo"[^>]*>([\s\S]*?)<\/(?:a|span)>/);
          if (brandM) product.brand = brandM[1].replace(/<[^>]+>/g,'').replace(/Visit the|Store/g,'').trim();

          // ══════════════════════════════════════════════════════════════
          // STEP 1: Extract the main twister/variation JSON blob
          // Amazon embeds ALL variation data in a data-a-dynamic-image attr
          // or in script tags as jQuery.parseJSON / P.register calls
          // ══════════════════════════════════════════════════════════════

          // Find the big twister data object — try multiple patterns
          let twisterJson = null;

          // Pattern A: "twister-js-init" script block
          const twisterScript = html.match(/var\s+dataToReturn\s*=\s*(\{[\s\S]*?\});\s*return dataToReturn/);
          if (twisterScript) { try { twisterJson = JSON.parse(twisterScript[1]); } catch {} }

          // Pattern B: jQuery.parseJSON embedded data
          if (!twisterJson) {
            const jqMatch = html.match(/jQuery\.parseJSON\s*\(\s*'([\s\S]*?)'\s*\)/);
            if (jqMatch) { try { twisterJson = JSON.parse(jqMatch[1].replace(/\\'/g,"'")); } catch {} }
          }

          // Pattern C: P.register('twister-js-init' ...) block — most common
          if (!twisterJson) {
            const pReg = html.match(/P\.register\s*\(\s*['"]twister[^'"]*['"]\s*,\s*function\s*\(\)\s*\{([\s\S]*?)\}\s*\)\s*;/);
            if (pReg) {
              const inner = pReg[1];
              const dvd = inner.match(/"dimensionValuesData"\s*:\s*(\{[\s\S]*?\})\s*,\s*"/);
              if (dvd) { try { twisterJson = { dimensionValuesData: JSON.parse(dvd[1]) }; } catch {} }
            }
          }

          // ══════════════════════════════════════════════════════════════
          // STEP 2: Extract asinVariationValues — the gold standard
          // Format: { "B0XXXXXX": { "color_name": "Red", "size_name": "M" }, ... }
          // ══════════════════════════════════════════════════════════════
          const asinToDims = {}; // asin -> { color_name, size_name, ... }
          const dimValToAsins = {}; // "color_name:Red" -> [asin, ...]

          // Try all known key names
          for (const key of ['asinVariationValues', 'asinToDimension', 'variationAsinBindings']) {
            const pat = new RegExp(`"${key}"\\s*:\\s*(\\{[\\s\\S]*?\\})(?=\\s*,\\s*")`);
            const m = html.match(pat);
            if (m) {
              try {
                const parsed = JSON.parse(m[1]);
                Object.assign(asinToDims, parsed);
                break;
              } catch {}
            }
          }

          // Build reverse index: dimension value -> list of ASINs
          for (const [asin, dims] of Object.entries(asinToDims)) {
            for (const [dimKey, dimVal] of Object.entries(dims)) {
              const k = `${dimKey}:${dimVal}`;
              if (!dimValToAsins[k]) dimValToAsins[k] = [];
              dimValToAsins[k].push(asin);
            }
          }

          // ══════════════════════════════════════════════════════════════
          // STEP 3: Extract per-ASIN prices
          // ══════════════════════════════════════════════════════════════
          const asinPrice = {}; // asin -> price string

          // Method A: priceToAsinList { "29.99": ["B0XX", ...] }
          const p2aM = html.match(/"priceToAsinList"\s*:\s*(\{[\s\S]*?\})(?=\s*,\s*")/);
          if (p2aM) {
            try {
              const p2a = JSON.parse(p2aM[1]);
              for (const [price, asins] of Object.entries(p2a)) {
                if (Array.isArray(asins)) asins.forEach(a => { if (!asinPrice[a]) asinPrice[a] = price; });
              }
            } catch {}
          }

          // Method B: scan for "B0XXXXXX"..."price"..."amount":N patterns
          const asinPriceInline = [...html.matchAll(/"([A-Z0-9]{10})"\s*[\s\S]{0,300}?"amount"\s*:\s*([\d.]+)/g)];
          asinPriceInline.forEach(m => { if (!asinPrice[m[1]]) asinPrice[m[1]] = m[2]; });

          // Method C: merchantCustomerPreferences or offerListings
          const offersM = html.match(/"offerListings"\s*:\s*(\[[\s\S]*?\])(?=\s*,\s*")/);
          if (offersM) {
            try {
              JSON.parse(offersM[1]).forEach(o => {
                if (o.asin && o.price?.amount) asinPrice[o.asin] = String(o.price.amount);
              });
            } catch {}
          }

          // Method D: dimensionValuesData from twister — has price per ASIN
          if (twisterJson?.dimensionValuesData) {
            for (const [asin, data] of Object.entries(twisterJson.dimensionValuesData)) {
              if (data?.price?.amount) asinPrice[asin] = String(data.price.amount);
            }
          }

          // ══════════════════════════════════════════════════════════════
          // STEP 4: Extract per-ASIN stock status
          // ══════════════════════════════════════════════════════════════
          const asinInStock = {}; // asin -> boolean

          // unavailableAsinSet
          const unavailM = html.match(/"unavailableAsinSet"\s*:\s*(\[[^\]]*\])/);
          if (unavailM) { try { JSON.parse(unavailM[1]).forEach(a => { asinInStock[a] = false; }); } catch {} }

          // inStockAsinSet
          const inStockM = html.match(/"inStockAsinSet"\s*:\s*(\[[^\]]*\])/);
          if (inStockM) { try { JSON.parse(inStockM[1]).forEach(a => { asinInStock[a] = true; }); } catch {} }

          // buyability field
          [...html.matchAll(/"asin"\s*:\s*"([A-Z0-9]{10})"[^}]*"buyability"\s*:\s*"([^"]+)"/g)]
            .forEach(m => { if (asinInStock[m[1]] === undefined) asinInStock[m[1]] = m[2] === 'BUYABLE'; });

          // ══════════════════════════════════════════════════════════════
          // STEP 5: Extract dimension names and their values
          // ══════════════════════════════════════════════════════════════
          const variationData = {}; // dimKey -> [val1, val2, ...]

          // Method A: variationValues block
          const varValsM = html.match(/"variationValues"\s*:\s*(\{[\s\S]*?\})(?=\s*,\s*")/);
          if (varValsM) {
            try {
              const vv = JSON.parse(varValsM[1]);
              for (const [k, vals] of Object.entries(vv)) {
                if (Array.isArray(vals) && vals.length) variationData[k] = vals;
              }
            } catch {}
          }

          // Method B: derive from asinToDims keys (always works if we have asin data)
          if (!Object.keys(variationData).length && Object.keys(asinToDims).length) {
            for (const dims of Object.values(asinToDims)) {
              for (const [dimKey, dimVal] of Object.entries(dims)) {
                if (!variationData[dimKey]) variationData[dimKey] = [];
                if (!variationData[dimKey].includes(dimVal)) variationData[dimKey].push(dimVal);
              }
            }
          }

          // Method C: HTML select dropdowns fallback
          for (const [htmlName, dimKey] of [
            ['dropdown_selected_size_name', 'size_name'],
            ['dropdown_selected_color_name', 'color_name'],
            ['dropdown_selected_style_name', 'style_name'],
          ]) {
            if (variationData[dimKey]) continue;
            const sel = html.match(new RegExp(`name="${htmlName}"[\\s\\S]*?<select[^>]*>([\\s\\S]*?)<\\/select>`, 'i'));
            if (sel) {
              const opts = [...sel[1].matchAll(/<option[^>]+value="([^"]+)"[^>]*>([^<]+)<\/option>/g)]
                .filter(m => m[1] && m[1] !== '-1' && !m[2].includes('Select'))
                .map(m => m[2].trim());
              if (opts.length) variationData[dimKey] = opts;
            }
          }

          // ══════════════════════════════════════════════════════════════
          // STEP 6: Extract images
          // ══════════════════════════════════════════════════════════════
          const colorImgMap = {}; // color value -> image URL
          const allImages = [];

          // colorImages block — most reliable
          const colorImagesM = html.match(/'colorImages'\s*:\s*(\{[\s\S]*?\})\s*,\s*'colorToAsin'/);
          if (colorImagesM) {
            try {
              const ci = JSON.parse(colorImagesM[1].replace(/'/g, '"').replace(/(\w+):/g, '"$1":'));
              for (const [key, imgs] of Object.entries(ci)) {
                if (!Array.isArray(imgs)) continue;
                if (key === 'initial') {
                  imgs.forEach(i => {
                    const src = i.hiRes || i.large;
                    if (src && !allImages.includes(src)) allImages.push(src);
                  });
                } else {
                  const best = imgs.find(i => i.hiRes) || imgs.find(i => i.large) || imgs[0];
                  if (best) colorImgMap[key] = best.hiRes || best.large || '';
                }
              }
            } catch {}
          }

          // Fallback: hiRes/large in scripts
          if (!allImages.length) {
            const hiRes = [...html.matchAll(/"hiRes"\s*:\s*"(https[^"]+)"/g)].map(m => m[1]);
            const large = [...html.matchAll(/"large"\s*:\s*"(https[^"]+)"/g)].map(m => m[1]);
            allImages.push(...[...new Set([...hiRes, ...large])].filter(s => s.includes('images-amazon')));
          }

          product.images = allImages.slice(0, 12);
          if (Object.keys(colorImgMap).length) product.variationImages['Color'] = colorImgMap;

          // ══════════════════════════════════════════════════════════════
          // STEP 7: Base price
          // ══════════════════════════════════════════════════════════════
          const priceSelectors = [
            /class="a-price-whole"[^>]*>\s*(\d[\d,]*)<\/span><span[^>]*class="a-price-fraction"[^>]*>\s*(\d+)/,
            /"priceAmount"\s*:\s*([\d.]+)/,
            /id="priceblock_ourprice"[^>]*>\s*\$?([\d,]+\.?\d*)/,
            /id="priceblock_dealprice"[^>]*>\s*\$?([\d,]+\.?\d*)/,
            /"buyingPrice"\s*:\s*([\d.]+)/,
            /class="a-offscreen"[^>]*>\$([\d,]+\.?\d*)/,
          ];
          for (const pat of priceSelectors) {
            const m = html.match(pat);
            if (m) { product.price = m[2] ? `${m[1].replace(/,/g,'')}.${m[2]}` : m[1].replace(/,/g,''); break; }
          }

          // ══════════════════════════════════════════════════════════════
          // STEP 8: Build variation groups with correct price + stock
          // ══════════════════════════════════════════════════════════════
          const dimKeyToLabel = {
            'color_name':'Color','size_name':'Size','style_name':'Style',
            'material_type':'Material','pattern_name':'Pattern',
            'configuration_name':'Configuration','edition_name':'Edition',
            'item_package_quantity':'Package Quantity','scent_name':'Scent',
            'flavor_name':'Flavor','hand_orientation_name':'Orientation',
          };

          for (const [dimKey, values] of Object.entries(variationData)) {
            if (!Array.isArray(values) || !values.length) continue;
            const label = dimKeyToLabel[dimKey] || dimKey.replace(/_name$/,'').replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase());

            const varValues = values.map(val => {
              const strVal = String(val);

              // Find all ASINs that have this dimension value
              const matchingAsins = dimValToAsins[`${dimKey}:${strVal}`] || [];

              // Also direct lookup from asinToDims
              if (!matchingAsins.length) {
                for (const [asin, dims] of Object.entries(asinToDims)) {
                  if (dims[dimKey] === strVal && !matchingAsins.includes(asin)) matchingAsins.push(asin);
                }
              }

              // Price: find cheapest available price among matching ASINs
              let varPrice = product.price || '0';
              const availablePrices = matchingAsins
                .filter(a => asinInStock[a] !== false && asinPrice[a])
                .map(a => parseFloat(asinPrice[a]))
                .filter(p => p > 0);
              if (availablePrices.length) varPrice = Math.min(...availablePrices).toFixed(2);
              else {
                // Try any price even out of stock
                const anyPrice = matchingAsins.map(a => parseFloat(asinPrice[a]||0)).filter(p=>p>0);
                if (anyPrice.length) varPrice = Math.min(...anyPrice).toFixed(2);
              }

              // Stock: in stock if ANY matching ASIN is available
              let inStock = true;
              if (matchingAsins.length > 0) {
                inStock = matchingAsins.some(a => asinInStock[a] !== false);
                // If none have explicit stock data, default to in stock
                if (!matchingAsins.some(a => asinInStock[a] !== undefined)) inStock = true;
              }

              // Image: color map or fallback to product image
              const img = dimKey === 'color_name' ? (colorImgMap[strVal] || allImages[0] || '') : '';

              return {
                value: strVal,
                price: varPrice,
                sourcePrice: varPrice,
                stock: inStock ? 10 : 0,
                inStock,
                asins: matchingAsins,
                image: img,
                enabled: inStock,
              };
            });

            product.variations.push({ name: label, values: varValues });
          }

          product.hasVariations = product.variations.length > 0;

          // Set base price = cheapest available variant
          if (product.hasVariations) {
            const prices = product.variations.flatMap(vg => vg.values)
              .filter(v => v.inStock && parseFloat(v.price) > 0)
              .map(v => parseFloat(v.price));
            if (prices.length) product.price = Math.min(...prices).toFixed(2);
          }

          // ── Description
          const bullets = [...html.matchAll(/<span class="a-list-item">\s*([\s\S]*?)\s*<\/span>/g)]
            .map(m => m[1].replace(/<[^>]+>/g,'').trim())
            .filter(b => b.length > 15 && b.length < 500 && !b.includes('{'))
            .slice(0, 5);
          if (bullets.length) product.description = bullets.join('\n');
          if (!product.description) {
            const feat = html.match(/id="feature-bullets"[^>]*>([\s\S]*?)<\/div>/);
            if (feat) product.description = feat[1].replace(/<[^>]+>/g,'').replace(/\s+/g,' ').trim().slice(0,1000);
          }

          // ── Item specifics
          const specTable = html.match(/id="productDetails_techSpec[^"]*"[^>]*>([\s\S]*?)<\/table>/i) ||
                            html.match(/id="detailBullets_feature_div"[^>]*>([\s\S]*?)<\/div>/i);
          if (specTable) {
            const rows = [...specTable[1].matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/g)];
            for (const row of rows.slice(0, 12)) {
              const cells = [...row[1].matchAll(/<t[hd][^>]*>([\s\S]*?)<\/t[hd]>/g)]
                .map(m => m[1].replace(/<[^>]+>/g,'').replace(/\u200f|\u200e/g,'').trim());
              if (cells.length >= 2 && cells[0] && cells[1] && cells[0].length < 60) {
                product.aspects[cells[0]] = [cells[1]];
              }
            }
          }

          // ── Debug info (remove in prod)
          product._debug = {
            asinCount: Object.keys(asinToDims).length,
            priceMapCount: Object.keys(asinPrice).length,
            stockMapCount: Object.keys(asinInStock).length,
            variationDims: Object.keys(variationData),
            imageCount: allImages.length,
            colorMapCount: Object.keys(colorImgMap).length,
          };
        }

        // ── WALMART ─────────────────────────────────────────────────────
        else if (url.includes('walmart.com')) {
          product.source = 'walmart';
          const nd = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
          if (nd) {
            try {
              const data = JSON.parse(nd[1])?.props?.pageProps?.initialData?.data?.product;
              if (data) {
                product.title = data.name || '';
                product.price = data.priceInfo?.currentPrice?.price?.toString() || '';
                product.brand = data.brand || '';
                product.description = data.shortDescription?.replace(/<[^>]+>/g,'') || '';
                if (data.imageInfo?.allImages) product.images = data.imageInfo.allImages.map(i=>i.url).filter(Boolean).slice(0,12);
                else if (data.imageInfo?.thumbnailUrl) product.images = [data.imageInfo.thumbnailUrl];
                if (data.variantCriteria) {
                  for (const crit of data.variantCriteria) {
                    const vg = {
                      name: crit.label || crit.id,
                      values: (crit.variantList||[]).map(v => ({
                        value: v.name || v.id,
                        price: v.priceInfo?.currentPrice?.price?.toString() || product.price,
                        stock: v.availabilityStatus === 'IN_STOCK' ? 10 : 0,
                        image: v.images?.[0]?.url || '',
                        enabled: true,
                      }))
                    };
                    if (vg.values.length > 0) product.variations.push(vg);
                    if ((crit.label||'').toLowerCase().includes('color')) {
                      const imgMap = {};
                      vg.values.forEach(v => { if(v.image) imgMap[v.value] = v.image; });
                      if (Object.keys(imgMap).length) product.variationImages['Color'] = imgMap;
                    }
                  }
                  product.hasVariations = product.variations.length > 0;
                }
                if (data.specifications) data.specifications.slice(0,10).forEach(s => { if(s.name&&s.value) product.aspects[s.name]=[s.value]; });
              }
            } catch {}
          }
          if (!product.title) { const m=html.match(/"name":"([^"]{10,200})"/); if(m) product.title=m[1]; }
          if (!product.price) { const m=html.match(/"price":(\d+\.?\d*)/); if(m) product.price=m[1]; }
          if (!product.images.length) { const imgs=[...html.matchAll(/"url":"(https:\/\/i5\.walmartimages\.com[^"]+)"/g)].map(m=>m[1]); product.images=[...new Set(imgs)].slice(0,12); }
        }

        // ── ALIEXPRESS ───────────────────────────────────────────────────
        else if (url.includes('aliexpress.com')) {
          product.source = 'aliexpress';
          const rp = html.match(/window\.runParams\s*=\s*(\{[\s\S]*?\});\s*\n/);
          if (rp) {
            try {
              const data = JSON.parse(rp[1]);
              const info = data?.data?.productInfoComponent || data?.data;
              if (info) {
                product.title = info.subject || info.title || '';
                product.price = info.priceComponent?.discountPrice?.formatedAmount?.replace(/[^0-9.]/g,'') ||
                                info.priceComponent?.originalPrice?.formatedAmount?.replace(/[^0-9.]/g,'') || '';
                product.description = info.description || '';
                if (info.imagePathList) product.images = info.imagePathList.map(i=>`https:${i}`).slice(0,12);
                const sku = data?.data?.skuComponent || info.skuComponent;
                if (sku?.productSKUPropertyList) {
                  for (const prop of sku.productSKUPropertyList) {
                    const vg = {
                      name: prop.skuPropertyName,
                      values: (prop.skuPropertyValues||[]).map(v => ({
                        value: v.propertyValueDisplayName || v.propertyValueName,
                        price: product.price, stock: 10,
                        image: v.skuPropertyImagePath ? `https:${v.skuPropertyImagePath}` : '',
                        enabled: true,
                      }))
                    };
                    if (vg.values.length > 0) product.variations.push(vg);
                    if (prop.skuPropertyName.toLowerCase().includes('color')) {
                      const imgMap = {};
                      vg.values.forEach(v => { if(v.image) imgMap[v.value] = v.image; });
                      if (Object.keys(imgMap).length) product.variationImages['Color'] = imgMap;
                    }
                  }
                  product.hasVariations = product.variations.length > 0;
                }
              }
            } catch {}
          }
          if (!product.title) { const m=html.match(/"subject":"([^"]+)"/); if(m) product.title=m[1]; }
          if (!product.price) { const m=html.match(/"minAmount":\{"value":(\d+\.?\d*)/); if(m) product.price=m[1]; }
          if (!product.images.length) { const imgs=[...html.matchAll(/"imageUrl":"(https?:\/\/ae[^"]+)"/g)].map(m=>m[1]); product.images=[...new Set(imgs)].slice(0,12); }
        }

        // ── WEBSTAURANTSTORE ─────────────────────────────────────────────
        else if (url.includes('webstaurantstore.com')) {
          product.source = 'webstaurantstore';
          const t = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
          if (t) product.title = t[1].replace(/<[^>]+>/g,'').trim();
          const pm = html.match(/\$(\d+\.\d{2})/); if (pm) product.price = pm[1];
          const imgs = [...html.matchAll(/src="(https:\/\/cdn[0-9]*\.webstaurantstore\.com\/images\/[^"]+\.(?:jpg|jpeg|png|webp))"/gi)].map(m=>m[1]);
          product.images = [...new Set(imgs)].filter(i=>!i.includes('icon')&&!i.includes('logo')).slice(0,12);
          const descM = html.match(/class="description[^"]*"[^>]*>([\s\S]*?)<\/div>/i);
          if (descM) product.description = descM[1].replace(/<[^>]+>/g,'').trim().slice(0,2000);
          const varTable = html.match(/<table[^>]*class="[^"]*variant[^"]*"[^>]*>([\s\S]*?)<\/table>/i);
          if (varTable) {
            const rows = [...varTable[1].matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/g)];
            const vals = [];
            for (const row of rows.slice(1)) {
              const cells = [...row[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/g)].map(m=>m[1].replace(/<[^>]+>/g,'').trim());
              if (cells.length >= 2 && cells[0]) {
                const pc = cells.find(c=>c.match(/\$[\d.]+/));
                vals.push({ value:cells[0], price:pc?pc.replace(/[^0-9.]/g,''):product.price, stock:10, image:'', enabled:true });
              }
            }
            if (vals.length > 0) { product.variations.push({ name:'Size', values:vals }); product.hasVariations = true; }
          }
          const specRows = [...html.matchAll(/<tr[^>]*>[\s\S]*?<th[^>]*>([\s\S]*?)<\/th>[\s\S]*?<td[^>]*>([\s\S]*?)<\/td>[\s\S]*?<\/tr>/g)];
          for (const row of specRows.slice(0,10)) {
            const k=row[1].replace(/<[^>]+>/g,'').trim(), v=row[2].replace(/<[^>]+>/g,'').trim();
            if (k&&v) product.aspects[k]=[v];
          }
        }

        return res.json({ success: true, product });
      } catch (e) {
        return res.json({ success: false, error: e.message, product });
      }
    }

    // ═══════════════════════════════════════════════════════════
    // PUSH TO EBAY
    // ═══════════════════════════════════════════════════════════
    if (action === 'push') {
      const { access_token, product } = body;
      if (!access_token || !product) return res.status(400).json({ error: 'Missing fields' });

      const groupSku = `DS-${Date.now()}-${Math.random().toString(36).slice(2,7).toUpperCase()}`;
      const authHeader = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json' };

      // Simple listing — no variations
      if (!product.hasVariations || !product.variations?.length) {
        const invRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(groupSku)}`, {
          method: 'PUT', headers: authHeader,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: parseInt(product.quantity)||10 } },
            condition: product.condition || 'NEW',
            product: { title: product.title.slice(0,80), description: product.description||product.title, imageUrls: (product.images||[]).slice(0,12), aspects: product.aspects||{} },
          }),
        });
        if (!invRes.ok) return res.status(400).json({ error: 'Inventory failed', details: await invRes.text() });
        const offerRes = await fetch(`${EBAY_API}/sell/inventory/v1/offer`, { method:'POST', headers:authHeader, body:JSON.stringify(buildOffer(groupSku, product)) });
        const offerData = await offerRes.json();
        if (!offerRes.ok) return res.status(400).json({ error: 'Offer failed', details: offerData });
        const pubRes = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerData.offerId}/publish`, { method:'POST', headers:authHeader });
        const pubData = await pubRes.json();
        return res.json({ success:true, sku:groupSku, offerId:offerData.offerId, listingId:pubData.listingId });
      }

      // Multi-variation listing
      const combos = buildCombos(product.variations);
      const createdSkus = [];

      for (const combo of combos) {
        if (combo.some(v => v.enabled === false)) continue;
        const varSku = `${groupSku}-${combo.map(v=>v.value.slice(0,5).replace(/[^a-zA-Z0-9]/g,'')).join('-')}`;
        const varPrice = combo.reduce((p,v) => v.price || p, product.price);
        const varStock = combo.reduce((s,v) => v.stock !== undefined ? v.stock : s, parseInt(product.quantity)||10);
        const varImages = getVarImages(combo, product.variationImages, product.images);
        const varAspects = { ...(product.aspects||{}) };
        combo.forEach(v => { varAspects[v.name] = [v.value]; });

        const invRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(varSku)}`, {
          method: 'PUT', headers: authHeader,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: Math.max(0, varStock) } },
            condition: product.condition || 'NEW',
            product: { title: product.title.slice(0,80), description: product.description||product.title, imageUrls: varImages.slice(0,12), aspects: varAspects },
          }),
        });
        if (invRes.ok) createdSkus.push(varSku);
      }

      if (!createdSkus.length) return res.status(400).json({ error: 'No variation SKUs created' });

      const groupRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`, {
        method: 'PUT', headers: authHeader,
        body: JSON.stringify({
          inventoryItemGroupKey: groupSku,
          title: product.title.slice(0,80),
          description: product.description || product.title,
          imageUrls: (product.images||[]).slice(0,12),
          aspects: product.aspects || {},
          variesBy: {
            aspectsImageVariesBy: Object.keys(product.variationImages||{}),
            specifications: product.variations.map(vg => ({
              name: vg.name,
              values: vg.values.filter(v=>v.enabled!==false).map(v=>v.value)
            })),
          },
        }),
      });
      if (!groupRes.ok) return res.status(400).json({ error: 'Group failed', details: await groupRes.text() });

      const offerBody = { ...buildOffer(groupSku, product), inventoryItemGroupKey: groupSku };
      delete offerBody.sku;
      const offerRes = await fetch(`${EBAY_API}/sell/inventory/v1/offer`, { method:'POST', headers:authHeader, body:JSON.stringify(offerBody) });
      const offerData = await offerRes.json();
      if (!offerRes.ok) return res.status(400).json({ error: 'Offer failed', details: offerData });

      const pubRes = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerData.offerId}/publish`, { method:'POST', headers:authHeader });
      const pubData = await pubRes.json();
      return res.json({ success:true, sku:groupSku, offerId:offerData.offerId, listingId:pubData.listingId, variationsCreated:createdSkus.length });
    }

    if (action === 'orders') {
      const token = req.query.access_token || body.access_token;
      const r = await fetch(`${EBAY_API}/sell/fulfillment/v1/order?limit=50`, { headers: { Authorization:`Bearer ${token}` } });
      return res.json(await r.json());
    }

    if (action === 'listings') {
      const token = req.query.access_token || body.access_token;
      const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=100`, { headers: { Authorization:`Bearer ${token}` } });
      return res.json(await r.json());
    }

    return res.status(400).json({ error: `Unknown action: ${action}` });

  } catch (err) {
    console.error('[DropSync Error]', err);
    return res.status(500).json({ error: err.message });
  }
};

function buildOffer(sku, product) {
  const p = { sku, marketplaceId:'EBAY_US', format:'FIXED_PRICE', listingDuration:'GTC', pricingSummary:{ price:{ value:String(parseFloat(product.price||0).toFixed(2)), currency:'USD' } }, categoryId:product.categoryId||'9355', merchantLocationKey:'default' };
  if (process.env.EBAY_FULFILLMENT_POLICY_ID) p.fulfillmentPolicyId = process.env.EBAY_FULFILLMENT_POLICY_ID;
  if (process.env.EBAY_PAYMENT_POLICY_ID)     p.paymentPolicyId     = process.env.EBAY_PAYMENT_POLICY_ID;
  if (process.env.EBAY_RETURN_POLICY_ID)      p.returnPolicyId      = process.env.EBAY_RETURN_POLICY_ID;
  return p;
}

function buildCombos(variations) {
  if (!variations?.length) return [];
  let combos = [[]];
  for (const vg of variations) {
    const next = [];
    for (const existing of combos)
      for (const v of (vg.values||[]))
        next.push([...existing, { name:vg.name, value:v.value, price:v.price, stock:v.stock, image:v.image, enabled:v.enabled }]);
    combos = next;
  }
  return combos;
}

function getVarImages(combo, variationImages, fallback) {
  const imgs = [];
  if (variationImages) for (const v of combo) { const gi = variationImages[v.name]; if (gi?.[v.value]) imgs.push(gi[v.value]); }
  for (const v of combo) if (v.image) imgs.push(v.image);
  return [...new Set([...imgs, ...(fallback||[])])];
}
