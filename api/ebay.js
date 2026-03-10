// DropSync AI Agent — Amazon → eBay Dropshipping Backend
// Clean architecture: per-ASIN prices+images, AI category detection, auto policies

const EBAY_API  = 'https://api.ebay.com';
const EBAY_AUTH = 'https://auth.ebay.com/oauth2/authorize';
const EBAY_TOK  = 'https://api.ebay.com/identity/v1/oauth2/token';
const SCOPES    = 'https://api.ebay.com/oauth/api_scope https://api.ebay.com/oauth/api_scope/sell.inventory https://api.ebay.com/oauth/api_scope/sell.account https://api.ebay.com/oauth/api_scope/sell.fulfillment https://api.ebay.com/oauth/api_scope/commerce.taxonomy.readonly';

const UA_LIST = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0',
];
const randUA = () => UA_LIST[Math.floor(Math.random() * UA_LIST.length)];

// ── Amazon page fetcher with retry + CAPTCHA detection ────────────────────────
async function fetchPage(url, ua) {
  for (let i = 0; i < 3; i++) {
    try {
      if (i > 0) await sleep(1500 * i);
      const r = await fetch(url, {
        headers: {
          'User-Agent': ua || randUA(),
          'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          'Accept-Language': 'en-US,en;q=0.9',
          'Cache-Control': 'no-cache',
          'Upgrade-Insecure-Requests': '1',
        },
        redirect: 'follow',
      });
      const html = await r.text();
      if (html.includes('Type the characters') || html.includes('robot check')) {
        if (i < 2) continue;
        return '';
      }
      return html;
    } catch (e) { if (i === 2) return ''; }
  }
  return '';
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── Extract price from Amazon HTML ────────────────────────────────────────────
function extractPrice(html) {
  const pats = [
    /class="a-price-whole"[^>]*>\s*(\d[\d,]*)<\/span><span[^>]*class="a-price-fraction"[^>]*>\s*(\d+)/,
    /"priceAmount"\s*:\s*([\d.]+)/,
    /class="a-offscreen"[^>]*>\$([\d,]+\.?\d*)/,
    /id="priceblock_ourprice"[^>]*>[^$]*\$([\d.]+)/,
    /"displayPrice"\s*:\s*"\$([\d,]+\.?\d*)"/,
  ];
  for (const p of pats) {
    const m = html.match(p);
    if (m) return parseFloat(m[2] ? `${m[1].replace(/,/g,'')}.${m[2]}` : m[1].replace(/,/g,''));
  }
  return null;
}

// ── Extract first hi-res image ────────────────────────────────────────────────
function extractMainImage(html) {
  const m = html.match(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/);
  return m ? m[1] : null;
}

// ── eBay Taxonomy API: get leaf category suggestions ─────────────────────────
async function getCategories(title, token) {
  try {
    const r = await fetch(
      `${EBAY_API}/commerce/taxonomy/v1/category_tree/0/get_category_suggestions?q=${encodeURIComponent(title.slice(0,80))}`,
      { headers: { Authorization: `Bearer ${token}`, 'Accept-Language': 'en-US' } }
    );
    const d = await r.json();
    return (d.categorySuggestions || []).slice(0, 10).map(s => ({
      id: s.category.categoryId,
      name: s.category.categoryName,
      path: (s.categoryTreeNodeAncestors || []).reverse().map(a => a.categoryName).join(' > '),
    }));
  } catch { return []; }
}

// ── Claude AI: pick category + write aspects + optimize title ─────────────────
async function aiEnrich(title, breadcrumbs, aspects, ebaySuggestions) {
  const key = process.env.ANTHROPIC_API_KEY;
  if (!key) return null;
  try {
    const r = await fetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'x-api-key': key, 'anthropic-version': '2023-06-01' },
      body: JSON.stringify({
        model: 'claude-sonnet-4-20250514', max_tokens: 800,
        messages: [{
          role: 'user',
          content: `You are an expert eBay listing optimizer. Given an Amazon product, choose the best eBay leaf category and write optimal item specifics.

Amazon Title: ${title}
Amazon Category Path: ${breadcrumbs.join(' > ')}
Amazon Item Specifics: ${JSON.stringify(aspects).slice(0,400)}
eBay Category Suggestions (id, name, path):
${ebaySuggestions.map(s => `  ${s.id}: ${s.name} (${s.path})`).join('\n')}

Return ONLY a JSON object (no markdown backticks):
{
  "categoryId": "<LEAF category ID from suggestions above>",
  "categoryName": "<category name>",
  "title": "<eBay-optimized title, under 80 chars, keyword-rich>",
  "aspects": {
    "Brand": ["Unbranded"],
    "Color": ["See Listing"],
    "Material": ["Polyester"],
    "Size": ["See Listing"],
    ... include 5-10 relevant aspects for this category
  }
}

CRITICAL: categoryId MUST be a LEAF category from the suggestions list. Never pick a parent category.`
        }]
      })
    });
    const d = await r.json();
    const text = (d.content?.[0]?.text || '').replace(/```json|```/g, '').trim();
    return JSON.parse(text);
  } catch (e) {
    console.warn('[AI] enrichment failed:', e.message);
    return null;
  }
}

// ── Resolve all 3 eBay policies, auto-create if missing ───────────────────────
async function resolvePolicies(token, supplied) {
  const auth = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };
  const p = {
    fulfillmentPolicyId: (supplied.fulfillmentPolicyId || '').trim(),
    paymentPolicyId:     (supplied.paymentPolicyId     || '').trim(),
    returnPolicyId:      (supplied.returnPolicyId       || '').trim(),
  };

  if (!p.fulfillmentPolicyId || !p.paymentPolicyId || !p.returnPolicyId) {
    const [fp, pp, rp] = await Promise.all([
      fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy?marketplace_id=EBAY_US`, { headers: auth }).then(r => r.json()).catch(() => ({})),
      fetch(`${EBAY_API}/sell/account/v1/payment_policy?marketplace_id=EBAY_US`,     { headers: auth }).then(r => r.json()).catch(() => ({})),
      fetch(`${EBAY_API}/sell/account/v1/return_policy?marketplace_id=EBAY_US`,      { headers: auth }).then(r => r.json()).catch(() => ({})),
    ]);
    if (!p.fulfillmentPolicyId) p.fulfillmentPolicyId = (fp.fulfillmentPolicies || [])[0]?.fulfillmentPolicyId || '';
    if (!p.paymentPolicyId)     p.paymentPolicyId     = (pp.paymentPolicies     || [])[0]?.paymentPolicyId     || '';
    if (!p.returnPolicyId)      p.returnPolicyId      = (rp.returnPolicies      || []).find(x => x.returnsAccepted)?.returnPolicyId
                                                        || (rp.returnPolicies || [])[0]?.returnPolicyId || '';
    console.log(`[policies] fp=${p.fulfillmentPolicyId?.slice(-8)} pp=${p.paymentPolicyId?.slice(-8)} rp=${p.returnPolicyId?.slice(-8)}`);
  }

  if (!p.fulfillmentPolicyId) {
    const r = await fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy`, {
      method: 'POST', headers: auth,
      body: JSON.stringify({
        name: 'DropSync Free Shipping', marketplaceId: 'EBAY_US',
        categoryTypes: [{ name: 'ALL_EXCLUDING_MOTORS_VEHICLES' }],
        handlingTime: { value: 3, unit: 'DAY' },
        shippingOptions: [{ optionType: 'DOMESTIC', costType: 'FLAT_RATE',
          shippingServices: [{ shippingServiceCode: 'USPSFirstClass', freeShipping: true,
            shippingCost: { value: '0.00', currency: 'USD' }, buyerResponsibleForShipping: false }] }],
      }),
    });
    const d = await r.json(); p.fulfillmentPolicyId = d.fulfillmentPolicyId || '';
  }
  if (!p.returnPolicyId) {
    const r = await fetch(`${EBAY_API}/sell/account/v1/return_policy`, {
      method: 'POST', headers: auth,
      body: JSON.stringify({
        name: 'DropSync 30-Day Returns', marketplaceId: 'EBAY_US',
        categoryTypes: [{ name: 'ALL_EXCLUDING_MOTORS_VEHICLES' }],
        returnsAccepted: true, returnPeriod: { value: 30, unit: 'DAY' },
        returnShippingCostPayer: 'BUYER', refundMethod: 'MONEY_BACK',
      }),
    });
    const d = await r.json(); p.returnPolicyId = d.returnPolicyId || '';
  }

  if (!p.fulfillmentPolicyId) throw new Error('No shipping policy found. Create one in eBay Seller Hub → Business Policies.');
  if (!p.returnPolicyId)      throw new Error('No return policy found. Create one in eBay Seller Hub → Business Policies.');
  return p;
}

// ── Ensure merchant location exists ──────────────────────────────────────────
async function ensureLocation(auth) {
  const r = await fetch(`${EBAY_API}/sell/inventory/v1/location`, { headers: auth }).then(r => r.json()).catch(() => ({}));
  if ((r.locations || []).length) return r.locations[0].merchantLocationKey;
  const key = 'MainWarehouse';
  await fetch(`${EBAY_API}/sell/inventory/v1/location/${key}`, {
    method: 'POST', headers: auth,
    body: JSON.stringify({ location: { address: { country: 'US' } }, locationTypes: ['WAREHOUSE'], name: key }),
  });
  return key;
}

// ── Build offer payload ───────────────────────────────────────────────────────
function buildOffer(sku, price, categoryId, policies, locationKey) {
  return {
    sku, marketplaceId: 'EBAY_US', format: 'FIXED_PRICE', listingDuration: 'GTC',
    pricingSummary: { price: { value: String(parseFloat(price || 0).toFixed(2)), currency: 'USD' } },
    categoryId: String(categoryId),
    merchantLocationKey: locationKey,
    listingPolicies: {
      fulfillmentPolicyId: policies.fulfillmentPolicyId,
      paymentPolicyId:     policies.paymentPolicyId,
      returnPolicyId:      policies.returnPolicyId,
    },
    tax: { applyTax: true },
  };
}

// ══════════════════════════════════════════════════════════════════════════════
module.exports = async (req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.status(200).end();

  const action = req.query.action || req.body?.action;
  const body   = req.body || {};

  try {

    // ── AUTH ──────────────────────────────────────────────────────────────────
    if (action === 'auth') {
      const REDIRECT = process.env.EBAY_REDIRECT_URI || `${req.headers['x-forwarded-proto']||'https'}://${req.headers.host}/api/ebay?action=callback`;
      const url = `${EBAY_AUTH}?client_id=${process.env.EBAY_CLIENT_ID}&redirect_uri=${encodeURIComponent(REDIRECT)}&response_type=code&scope=${encodeURIComponent(SCOPES)}`;
      return res.json({ url });
    }

    if (action === 'callback') {
      const REDIRECT = process.env.EBAY_REDIRECT_URI || `${req.headers['x-forwarded-proto']||'https'}://${req.headers.host}/api/ebay?action=callback`;
      const creds = Buffer.from(`${process.env.EBAY_CLIENT_ID}:${process.env.EBAY_CLIENT_SECRET}`).toString('base64');
      const r = await fetch(EBAY_TOK, {
        method: 'POST',
        headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `grant_type=authorization_code&code=${encodeURIComponent(req.query.code)}&redirect_uri=${encodeURIComponent(REDIRECT)}`,
      });
      const d = await r.json();
      return res.setHeader('Content-Type','text/html').send(
        `<!DOCTYPE html><html><body><script>
          window.opener?.postMessage({type:'ebay_auth',token:'${d.access_token}',refresh:'${d.refresh_token}',expiry:${Date.now()+((d.expires_in||7200)-120)*1000}},'*');
          document.body.innerHTML='<p style="font-family:sans-serif;padding:40px">✅ Connected to eBay! You can close this window.</p>';
        </script></body></html>`
      );
    }

    if (action === 'refresh') {
      const creds = Buffer.from(`${process.env.EBAY_CLIENT_ID}:${process.env.EBAY_CLIENT_SECRET}`).toString('base64');
      const r = await fetch(EBAY_TOK, {
        method: 'POST',
        headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `grant_type=refresh_token&refresh_token=${encodeURIComponent(body.refresh_token)}&scope=${encodeURIComponent(SCOPES)}`,
      });
      const d = await r.json();
      if (!d.access_token) return res.status(400).json({ error: 'Token refresh failed', raw: d });
      return res.json({ access_token: d.access_token, expires_in: d.expires_in, expiry: Date.now() + ((d.expires_in||7200)-120)*1000 });
    }

    // ── POLICIES ─────────────────────────────────────────────────────────────
    if (action === 'policies') {
      const token = body.access_token || req.query.access_token;
      if (!token) return res.status(400).json({ error: 'No token' });
      const auth = { Authorization: `Bearer ${token}`, 'Accept-Language': 'en-US' };
      const [fp, pp, rp] = await Promise.all([
        fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy?marketplace_id=EBAY_US`, { headers: auth }).then(r => r.json()),
        fetch(`${EBAY_API}/sell/account/v1/payment_policy?marketplace_id=EBAY_US`,     { headers: auth }).then(r => r.json()),
        fetch(`${EBAY_API}/sell/account/v1/return_policy?marketplace_id=EBAY_US`,      { headers: auth }).then(r => r.json()),
      ]);
      return res.json({
        fulfillment: (fp.fulfillmentPolicies||[]).map(p => ({ id: p.fulfillmentPolicyId, name: p.name })),
        payment:     (pp.paymentPolicies||[]).map(p => ({ id: p.paymentPolicyId, name: p.name })),
        return:      (rp.returnPolicies||[]).map(p => ({ id: p.returnPolicyId, name: p.name })),
      });
    }

    // ── SCRAPE: Amazon → structured product data ──────────────────────────────
    if (action === 'scrape') {
      let url = (body.url || req.query.url || '').trim();
      if (!url) return res.status(400).json({ error: 'No URL provided' });

      // Normalize to clean dp/ASIN URL
      const asinM = url.match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})/);
      const asinP = url.match(/[?&]asin=([A-Z0-9]{10})/i);
      const asin  = asinM?.[1] || asinP?.[1];
      if (asin) url = `https://www.amazon.com/dp/${asin}`;

      const ua   = randUA();
      const html = await fetchPage(url, ua);
      if (!html) return res.json({ success: false, error: 'Could not load page — Amazon may be rate limiting. Try again in 1 min.' });

      const product = {
        url, source: 'amazon', asin: asin || '',
        title: '', price: 0, images: [],
        description: '', aspects: {}, breadcrumbs: [],
        variations: [], variationImages: {}, hasVariations: false,
        inStock: true, quantity: 10,
      };

      // Title
      const tM = html.match(/id="productTitle"[^>]*>\s*([^<]{5,})/);
      if (tM) product.title = tM[1].trim().replace(/\s+/g,' ');

      // Breadcrumbs (Amazon category path)
      const bcRaw = [...html.matchAll(/class="a-link-normal"[^>]*>\s*([^<]{2,40})\s*<\/a>/g)];
      product.breadcrumbs = bcRaw.slice(0, 6).map(m => m[1].trim()).filter(s => s.length > 1 && !/^\d+$/.test(s));

      // Price
      product.price = extractPrice(html) || 0;

      // Stock
      product.inStock = !html.toLowerCase().includes('currently unavailable');

      // Images — all hiRes from page
      const imgs = [...html.matchAll(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/g)]
        .map(m => m[1]).filter((v,i,a) => a.indexOf(v)===i);
      product.images = imgs.slice(0, 12);

      // Description from bullet points
      const bullets = [...html.matchAll(/<span class="a-list-item">\s*([^<]{20,500})\s*<\/span>/g)]
        .map(m => m[1].trim()).slice(0, 5);
      product.description = bullets.join('\n') || product.title;

      // Item specifics from product details table
      const rows = [...html.matchAll(/<th[^>]*class="[^"]*prodDetSectionEntry[^"]*"[^>]*>([^<]+)<\/th>\s*<td[^>]*>([^<]+)<\/td>/gi)];
      for (const [, k, v] of rows) {
        const key = k.trim(), val = v.trim().replace(/\s+/g,' ');
        if (key && val && val.length < 120 && !val.includes('›')) product.aspects[key] = [val];
      }
      // Also grab from feature bullets: "Brand: X"
      for (const [, k, v] of [...html.matchAll(/(Brand|Material|Color|Size|Style|Weight|Dimensions?)\s*:\s*([^\n<]{2,60})/g)]) {
        if (!product.aspects[k]) product.aspects[k] = [v.trim()];
      }

      // ── Variation data ────────────────────────────────────────────────────
      // 1) variationValues — gives us all color/size names
      let varVals = null;
      const vvM = html.match(/"variationValues"\s*:\s*(\{"[a-z_]+"\s*:\s*\[[^\]]+\][^}]*\})/);
      if (vvM) try { varVals = JSON.parse(vvM[1]); } catch {}

      // 2) ASIN → {color, size} mapping from HTML
      const asinMap = {};  // asin → { color, size }
      const dimMatches = [...html.matchAll(/"([A-Z0-9]{10})"\s*:\s*\{([^}]{0,300})\}/g)];
      for (const [, a, body] of dimMatches) {
        const cM = body.match(/"color_name"\s*:\s*"([^"]+)"/);
        const sM = body.match(/"size_name"\s*:\s*"([^"]+)"/);
        if (cM || sM) asinMap[a] = { color: cM?.[1]||null, size: sM?.[1]||null };
      }

      // 3) colorToAsin from data-asin attributes on swatch buttons
      const colorToAsin = {};
      const swatchRe = /data-dp-url[^>]*th=([A-Z0-9]{10})[^>]*(?:title|alt)="([^"]+)"/g;
      for (const [, a, name] of [...html.matchAll(swatchRe)]) {
        if (name && name.length < 60 && !colorToAsin[name]) colorToAsin[name] = a;
      }
      // Also try: id="color_name_X_B0XXXXXX" style elements
      const swRe2 = /data-asin="([A-Z0-9]{10})"[^>]*data-dp-url/g;
      // And from the ASIN map
      for (const [a, dims] of Object.entries(asinMap)) {
        if (dims.color && !colorToAsin[dims.color]) colorToAsin[dims.color] = a;
      }

      // Also look for explicit colorToAsin JSON object
      const ctaM = html.match(/"colorToAsin"\s*:\s*(\{[^}]{0,2000}\})/);
      if (ctaM) try { Object.assign(colorToAsin, JSON.parse(ctaM[1])); } catch {}

      const hasVar = !!(varVals && (varVals.color_name?.length || varVals.size_name?.length));
      product.hasVariations = hasVar;

      if (hasVar) {
        const colors = varVals.color_name || [];
        const sizes  = varVals.size_name  || [];

        // Resolve which ASIN to fetch for each color
        const colorASINs = colors.map(c => ({ color: c, asin: colorToAsin[c] || null })).filter(x => x.asin);

        console.log(`[scrape] ${colors.length} colors, ${sizes.length} sizes, ${colorASINs.length} ASINs to fetch`);

        // Fetch each color's ASIN page → get image + price
        const colorData = {}; // color → { image, price, inStock }
        if (colorASINs.length) {
          await Promise.all(colorASINs.slice(0, 15).map(async ({ color, asin }) => {
            const h = await fetchPage(`https://www.amazon.com/dp/${asin}`, ua);
            if (!h) return;
            const img   = extractMainImage(h);
            const price = extractPrice(h);
            const stock = !h.toLowerCase().includes('currently unavailable');
            colorData[color] = { image: img || '', price: price || 0, inStock: stock };
            if (img && !product.images.includes(img)) product.images.push(img);
          }));
        }

        // Fallback: if we couldn't fetch ASINs, use page images in order
        if (!Object.keys(colorData).length && product.images.length) {
          colors.forEach((c, i) => {
            colorData[c] = { image: product.images[i] || product.images[0] || '', price: product.price, inStock: true };
          });
        }

        // Build variation groups
        if (colors.length) {
          product.variations.push({
            name: 'Color',
            values: colors.map(c => ({
              value:   c,
              price:   colorData[c]?.price   || product.price || 0,
              image:   colorData[c]?.image   || '',
              inStock: colorData[c]?.inStock ?? true,
              enabled: true,
            })),
          });
          product.variationImages['Color'] = Object.fromEntries(
            Object.entries(colorData).map(([c, d]) => [c, d.image]).filter(([, img]) => img)
          );
        }
        if (sizes.length) {
          product.variations.push({
            name: 'Size',
            values: sizes.map(s => ({
              value: s, price: product.price || 0,
              image: '', inStock: true, enabled: true,
            })),
          });
        }

        // Set product price to minimum color price
        const colorPrices = colors.map(c => colorData[c]?.price).filter(Boolean);
        if (colorPrices.length && !product.price) product.price = Math.min(...colorPrices);
      }

      console.log(`[scrape] OK "${product.title.slice(0,50)}" price=$${product.price} colors=${product.variations.find(v=>v.name==='Color')?.values.length||0} imgs=${product.images.length}`);
      return res.json({ success: true, product });
    }

    // ── PUSH: create eBay listing ─────────────────────────────────────────────
    if (action === 'push') {
      const { access_token, product, fulfillmentPolicyId, paymentPolicyId, returnPolicyId } = body;
      if (!access_token || !product) return res.status(400).json({ error: 'Missing access_token or product' });

      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Content-Language': 'en-US', 'Accept-Language': 'en-US' };
      console.log(`[push] "${product.title?.slice(0,60)}" hasVariations=${product.hasVariations}`);

      // Resolve policies
      let policies;
      try { policies = await resolvePolicies(access_token, { fulfillmentPolicyId, paymentPolicyId, returnPolicyId }); }
      catch (e) { return res.status(400).json({ error: e.message }); }

      // AI category + aspects
      const suggestions = await getCategories(product.title || '', access_token);
      const ai = await aiEnrich(product.title, product.breadcrumbs || [], product.aspects || {}, suggestions);
      const categoryId = ai?.categoryId || suggestions[0]?.id || '11450';
      const listingTitle = (ai?.title || product.title || 'Product').slice(0, 80);
      const aspects = { ...(product.aspects || {}), ...(ai?.aspects || {}) };
      console.log(`[push] cat=${categoryId} "${listingTitle.slice(0,50)}"`);

      // Merchant location
      const locationKey = await ensureLocation(auth);
      const basePrice   = parseFloat(product.price || 0).toFixed(2);
      const groupSku    = `DS-${Date.now()}-${Math.random().toString(36).slice(2,7).toUpperCase()}`;

      // ── SIMPLE LISTING ─────────────────────────────────────────────────────
      if (!product.hasVariations || !product.variations?.length) {
        const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(groupSku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: parseInt(product.quantity)||10 } },
            condition: 'NEW',
            product: { title: listingTitle, description: product.description || listingTitle, imageUrls: product.images.slice(0,12), aspects },
          }),
        });
        if (!ir.ok) return res.status(400).json({ error: 'Inventory PUT failed', details: await ir.text() });

        const or = await fetch(`${EBAY_API}/sell/inventory/v1/offer`, {
          method: 'POST', headers: auth,
          body: JSON.stringify(buildOffer(groupSku, basePrice, categoryId, policies, locationKey)),
        });
        const od = await or.json();
        if (!or.ok) return res.status(400).json({ error: 'Offer failed', details: od });

        const pr  = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${od.offerId}/publish`, { method: 'POST', headers: auth });
        const pd  = await pr.json();
        return res.json({ success: true, sku: groupSku, offerId: od.offerId, listingId: pd.listingId });
      }

      // ── VARIATION LISTING ──────────────────────────────────────────────────
      const colorGroup = product.variations.find(v => /color|colour/i.test(v.name));
      const sizeGroup  = product.variations.find(v => /size/i.test(v.name));
      const colorImgs  = product.variationImages?.['Color'] || {};

      // Build flat variant list
      const variants = [];
      const mkSku = parts => `${groupSku}-${parts.join('-').replace(/[^A-Z0-9]/gi,'_').toUpperCase().slice(0,40)}`;

      if (colorGroup && sizeGroup) {
        for (const cv of colorGroup.values.filter(v => v.enabled!==false)) {
          for (const sv of sizeGroup.values.filter(v => v.enabled!==false)) {
            variants.push({ sku: mkSku([cv.value, sv.value]), color: cv.value, size: sv.value,
              price: parseFloat(cv.price || basePrice).toFixed(2),
              image: colorImgs[cv.value] || product.images[0] || '',
              qty:   cv.inStock ? (parseInt(product.quantity)||10) : 0 });
          }
        }
      } else if (colorGroup) {
        for (const cv of colorGroup.values.filter(v => v.enabled!==false)) {
          variants.push({ sku: mkSku([cv.value]), color: cv.value, size: null,
            price: parseFloat(cv.price || basePrice).toFixed(2),
            image: colorImgs[cv.value] || product.images[0] || '',
            qty:   cv.inStock ? (parseInt(product.quantity)||10) : 0 });
        }
      } else if (sizeGroup) {
        for (const sv of sizeGroup.values.filter(v => v.enabled!==false)) {
          variants.push({ sku: mkSku([sv.value]), color: null, size: sv.value,
            price: parseFloat(sv.price || basePrice).toFixed(2),
            image: product.images[0] || '', qty: parseInt(product.quantity)||10 });
        }
      }

      const final = variants.slice(0, 250);
      console.log(`[push] ${final.length} variants`);

      // PUT each inventory_item (batched)
      let okInv = 0;
      for (let i = 0; i < final.length; i += 8) {
        await Promise.all(final.slice(i, i+8).map(async v => {
          const asp = { ...aspects };
          if (v.color) asp['Color'] = [v.color];
          if (v.size)  asp['Size']  = [v.size];
          const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(v.sku)}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({
              availability: { shipToLocationAvailability: { quantity: v.qty } },
              condition: 'NEW',
              product: {
                title: listingTitle, description: product.description || listingTitle,
                imageUrls: [v.image, ...product.images.filter(x => x !== v.image)].filter(Boolean).slice(0, 12),
                aspects: asp,
              },
            }),
          });
          if (r.ok || r.status === 204) okInv++;
          else console.warn(`[push] inv ${v.sku.slice(-15)}: ${r.status}`);
        }));
        if (i+8 < final.length) await sleep(150);
      }
      console.log(`[push] inventory items: ${okInv}/${final.length}`);

      // PUT inventory_item_group
      const varAspects = {};
      if (colorGroup) varAspects['Color'] = colorGroup.values.map(v => v.value);
      if (sizeGroup)  varAspects['Size']  = sizeGroup.values.map(v => v.value);

      let groupOk = false;
      for (let attempt = 1; attempt <= 3 && !groupOk; attempt++) {
        const gr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({
            inventoryItemGroupKey: groupSku, title: listingTitle,
            description: product.description || listingTitle,
            imageUrls: product.images.slice(0, 12),
            variantSKUs: final.map(v => v.sku),
            aspects: varAspects, variesBy: Object.keys(varAspects),
          }),
        });
        if (gr.ok || gr.status === 204) { groupOk = true; console.log('[push] group ok'); }
        else {
          const gt = await gr.text();
          console.warn(`[push] group attempt ${attempt}: ${gr.status}`, gt.slice(0,200));
          if (attempt < 3) await sleep(600); else return res.status(400).json({ error: 'Group PUT failed', details: gt.slice(0,400) });
        }
      }

      // Bulk create offers (25 at a time)
      const allOfferIds = [];
      for (let i = 0; i < final.length; i += 25) {
        const batch = final.slice(i, i+25).map(v => buildOffer(v.sku, v.price, categoryId, policies, locationKey));
        const or = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_create_offer`, {
          method: 'POST', headers: auth, body: JSON.stringify({ requests: batch }),
        });
        const od = await or.json();
        for (const resp of (od.responses || [])) { if (resp.offerId) allOfferIds.push(resp.offerId); }
        console.log(`[push] offers batch ${Math.floor(i/25)+1}: ${(od.responses||[]).filter(r=>r.offerId).length} ok`);
      }

      // Publish by group
      for (let attempt = 1; attempt <= 3; attempt++) {
        const pr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/publish_by_inventory_item_group`, {
          method: 'POST', headers: auth,
          body: JSON.stringify({ inventoryItemGroupKey: groupSku, marketplaceId: 'EBAY_US' }),
        });
        const pd = await pr.json();
        if (pd.listingId) {
          console.log(`[push] published! listingId=${pd.listingId}`);
          return res.json({ success: true, sku: groupSku, listingId: pd.listingId, variantsCreated: final.length });
        }
        const errId = (pd.errors||[])[0]?.errorId;
        console.warn(`[push] publish attempt ${attempt}: ${JSON.stringify(pd).slice(0,300)}`);
        if (attempt === 3) return res.status(400).json({ error: 'Publish failed', details: pd, errorId: errId });
        // Add missing aspects if required
        if (errId === 25004 || errId === 25003) {
          for (const param of (pd.errors||[])[0]?.parameters||[]) {
            if (!aspects[param.value]) aspects[param.value] = ['Unbranded'];
          }
        }
        await sleep(800);
      }
    }

    // ── SYNC: re-scrape Amazon, push diffs to eBay ────────────────────────────
    if (action === 'sync') {
      const { access_token, ebaySku, sourceUrl, hasVariations, quantity, variations, variationImages } = body;
      if (!access_token || !ebaySku) return res.status(400).json({ error: 'Missing fields' });
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };

      // Re-scrape to get fresh data
      const ua   = randUA();
      const html = await fetchPage(sourceUrl, ua);
      if (!html) return res.json({ success: false, error: 'Could not re-fetch Amazon page' });

      const freshPrice = extractPrice(html);
      const freshStock = !html.toLowerCase().includes('currently unavailable');

      if (!hasVariations) {
        const offerList = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(ebaySku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
        const offerId = (offerList.offers||[])[0]?.offerId;
        if (offerId && freshPrice) {
          await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerId}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({ pricingSummary: { price: { value: String(freshPrice.toFixed(2)), currency: 'USD' } } }),
          });
        }
        await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({ availability: { shipToLocationAvailability: { quantity: freshStock ? parseInt(quantity)||10 : 0 } }, condition: 'NEW', product: {} }),
        });
        return res.json({ success: true, price: freshPrice, inStock: freshStock });
      }

      // Variation sync: update each color ASIN's price/stock
      const colorGroup = (variations||[]).find(v => /color/i.test(v.name));
      const colorImgs  = variationImages?.['Color'] || {};
      const colorToAsin = {};
      // Re-extract ASIN map
      const dimMatches = [...html.matchAll(/"([A-Z0-9]{10})"\s*:\s*\{([^}]{0,200})\}/g)];
      for (const [,a,b] of dimMatches) {
        const cM = b.match(/"color_name"\s*:\s*"([^"]+)"/);
        if (cM) colorToAsin[cM[1]] = a;
      }

      const updated = [];
      if (colorGroup?.values?.length) {
        // Fetch prices for each color ASIN
        await Promise.all(colorGroup.values.slice(0,12).map(async cv => {
          const asin = colorToAsin[cv.value];
          if (!asin) return;
          const h = await fetchPage(`https://www.amazon.com/dp/${asin}`, ua);
          if (!h) return;
          const p = extractPrice(h);
          const s = !h.toLowerCase().includes('currently unavailable');
          if (p) updated.push({ color: cv.value, price: p, inStock: s });
        }));
      }

      // Push updates to eBay inventory items
      const groupRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
      const skus = groupRes.variantSKUs || [];
      let syncCount = 0;
      for (const sku of skus.slice(0, 100)) {
        const skuLower = sku.toLowerCase();
        const match = updated.find(u => skuLower.includes(u.color.replace(/\s+/g,'_').toLowerCase()));
        if (!match) continue;
        // Update inventory item qty
        await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({ availability: { shipToLocationAvailability: { quantity: match.inStock ? parseInt(quantity)||10 : 0 } }, condition: 'NEW', product: {} }),
        });
        // Update offer price
        const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
        const oid = (ol.offers||[])[0]?.offerId;
        if (oid) {
          await fetch(`${EBAY_API}/sell/inventory/v1/offer/${oid}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({ pricingSummary: { price: { value: String(match.price.toFixed(2)), currency: 'USD' } } }),
          });
        }
        syncCount++;
      }
      return res.json({ success: true, updatedVariants: syncCount, freshData: updated });
    }

    // ── END LISTING ───────────────────────────────────────────────────────────
    if (action === 'endListing') {
      const { access_token, ebaySku } = body;
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };
      const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(ebaySku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
      let ended = 0;
      for (const offer of (ol.offers||[])) {
        if (offer.status === 'PUBLISHED') {
          await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offer.offerId}/withdraw`, { method: 'POST', headers: auth });
          ended++;
        }
      }
      return res.json({ success: true, ended });
    }

    return res.status(400).json({ error: `Unknown action: ${action}` });

  } catch (err) {
    console.error('[Error]', err.message);
    return res.status(500).json({ error: err.message });
  }
};
