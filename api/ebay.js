// DropSync AI Agent — Amazon → eBay Dropshipping Backend
// Clean architecture: per-ASIN prices+images, AI category detection, auto policies

const SCOPES = 'https://api.ebay.com/oauth/api_scope https://api.ebay.com/oauth/api_scope/sell.inventory https://api.ebay.com/oauth/api_scope/sell.account https://api.ebay.com/oauth/api_scope/sell.fulfillment';

function getEbayUrls(sandbox) {
  return {
    EBAY_API:     sandbox ? 'https://api.sandbox.ebay.com'                           : 'https://api.ebay.com',
    EBAY_AUTH:    sandbox ? 'https://auth.sandbox.ebay.com/oauth2/authorize'         : 'https://auth.ebay.com/oauth2/authorize',
    EBAY_TOK:     sandbox ? 'https://api.sandbox.ebay.com/identity/v1/oauth2/token'  : 'https://api.ebay.com/identity/v1/oauth2/token',
    CLIENT_ID:    sandbox ? process.env.EBAY_SANDBOX_CLIENT_ID     : process.env.EBAY_CLIENT_ID,
    CLIENT_SECRET:sandbox ? process.env.EBAY_SANDBOX_CLIENT_SECRET : process.env.EBAY_CLIENT_SECRET,
    REDIRECT:     sandbox ? (process.env.EBAY_SANDBOX_REDIRECT_URI || process.env.EBAY_REDIRECT_URI) : process.env.EBAY_REDIRECT_URI,
  };
}

const UA_LIST = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
];
const randUA = () => UA_LIST[Math.floor(Math.random() * UA_LIST.length)];

// ── Amazon page fetcher: direct + proxy fallbacks ─────────────────────────────
async function fetchPage(url, ua) {
  const isBlocked = (html) =>
    !html ||
    html.includes('Type the characters') ||
    html.includes('robot check') ||
    html.includes('Enter the characters') ||
    html.includes('automated access') ||
    html.includes('api-services-support@amazon.com') ||
    (html.length < 8000 && !html.includes('productTitle'));

  const directHeaders = (agent) => ({
    'User-Agent': agent || ua || randUA(),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Sec-CH-UA': '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    'Sec-CH-UA-Mobile': '?0',
    'Sec-CH-UA-Platform': '"Windows"',
  });

  // Strategy 1-3: direct fetch with different UAs and URL variants
  const urlVariants = [url, url.replace('?th=1','?psc=1'), url.replace('?th=1','')];
  for (let i = 0; i < 3; i++) {
    try {
      if (i > 0) await sleep(600 + i * 800);
      const r = await fetch(urlVariants[i] || url, { headers: directHeaders(UA_LIST[i % UA_LIST.length]), redirect: 'follow' });
      const html = await r.text();
      if (!isBlocked(html)) { console.log(`[fetch] direct ok attempt ${i+1} len=${html.length}`); return html; }
      console.warn(`[fetch] direct attempt ${i+1} blocked (${html.length}b)`);
    } catch (e) { console.warn(`[fetch] direct attempt ${i+1} error: ${e.message}`); }
  }

  // Strategy 4: allorigins proxy (free CORS proxy that relays the page)
  try {
    await sleep(1000);
    const proxyUrl = `https://api.allorigins.win/get?url=${encodeURIComponent(url)}`;
    const r = await fetch(proxyUrl, { headers: { 'User-Agent': randUA() } });
    const d = await r.json();
    const html = d.contents || '';
    if (!isBlocked(html)) { console.log(`[fetch] allorigins ok len=${html.length}`); return html; }
    console.warn(`[fetch] allorigins blocked (${html.length}b)`);
  } catch (e) { console.warn(`[fetch] allorigins error: ${e.message}`); }

  // Strategy 5: scraperapi free tier
  try {
    await sleep(800);
    const scraperUrl = `https://api.scraperapi.com/?url=${encodeURIComponent(url)}&render=false`;
    const r = await fetch(scraperUrl, { headers: { 'User-Agent': randUA() } });
    const html = await r.text();
    if (!isBlocked(html)) { console.log(`[fetch] scraperapi ok len=${html.length}`); return html; }
    console.warn(`[fetch] scraperapi blocked (${html.length}b)`);
  } catch (e) { console.warn(`[fetch] scraperapi error: ${e.message}`); }

  // Strategy 6: codetabs proxy
  try {
    await sleep(1200);
    const r = await fetch(`https://api.codetabs.com/v1/proxy?quest=${encodeURIComponent(url)}`, {
      headers: { 'User-Agent': randUA() }
    });
    const html = await r.text();
    if (!isBlocked(html)) { console.log(`[fetch] codetabs ok len=${html.length}`); return html; }
    console.warn(`[fetch] codetabs blocked (${html.length}b)`);
  } catch (e) { console.warn(`[fetch] codetabs error: ${e.message}`); }

  // Strategy 7: thingproxy
  try {
    await sleep(1000);
    const r = await fetch(`https://thingproxy.freeboard.io/fetch/${url}`, {
      headers: { 'User-Agent': randUA(), 'Accept': 'text/html' }
    });
    const html = await r.text();
    if (!isBlocked(html)) { console.log(`[fetch] thingproxy ok len=${html.length}`); return html; }
    console.warn(`[fetch] thingproxy blocked (${html.length}b)`);
  } catch (e) { console.warn(`[fetch] thingproxy error: ${e.message}`); }

  // Strategy 8: htmlpreview / corsproxy.io
  try {
    await sleep(1000);
    const r = await fetch(`https://corsproxy.io/?${encodeURIComponent(url)}`, {
      headers: { 'User-Agent': randUA(), 'Accept': 'text/html,application/xhtml+xml' }
    });
    const html = await r.text();
    if (!isBlocked(html)) { console.log(`[fetch] corsproxy ok len=${html.length}`); return html; }
    console.warn(`[fetch] corsproxy blocked (${html.length}b)`);
  } catch (e) { console.warn(`[fetch] corsproxy error: ${e.message}`); }

  return '';
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

// ── eBay title sanitizer — strips words that trigger policy violations ─────────
function sanitizeTitle(title) {
  if (!title) return '';
  // Words eBay bans or that trigger errorId 25019
  const BANNED = [
    /\bauthentic\b/gi, /\bgenuine\b/gi, /\boriginal\b/gi,
    /\breal\b/gi, /\bverified\b/gi, /\bcertified\b/gi,
    /\bauthorized\b/gi, /\bofficially licensed\b/gi,
    /\bnot fake\b/gi, /\bnot a fake\b/gi, /\bnot replica\b/gi,
  ];
  let t = title;
  for (const re of BANNED) t = t.replace(re, '');
  // Collapse multiple spaces
  t = t.replace(/\s{2,}/g, ' ').trim();
  // Remove trailing punctuation/separators
  t = t.replace(/[\-–,|:]+$/, '').trim();
  // Enforce 80 char eBay limit
  if (t.length > 80) t = t.slice(0, 80).replace(/\s+\S*$/, '').trim();
  return t;
}

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

// ── Extract Amazon shipping cost from product page HTML ──────────────────────
// Returns: 0 if FREE shipping, positive number if paid shipping
function extractShippingCost(html) {
  // FREE shipping/delivery patterns
  const freePatterns = [
    /FREE\s+(?:delivery|shipping)/i,
    /free\s+returns/i,
    /"isFreeShipping"\s*:\s*true/i,
  ];
  for (const p of freePatterns) {
    if (p.test(html)) return 0;
  }
  // Paid shipping patterns: "$4.99 shipping", "+ $6.99 shipping", "Ships for $3.99"
  const paidPatterns = [
    /\+\s*\$([\d.]+)\s+shipping/i,
    /\$([\d.]+)\s+shipping/i,
    /"shippingAmount"\s*:\s*\{"amount"\s*:\s*([\d.]+)/,
    /"deliveryPrice"\s*:\s*\{"amount"\s*:\s*([\d.]+)/,
    /shipping\s*&amp;\s*handling[^$]*\$([\d.]+)/i,
  ];
  for (const p of paidPatterns) {
    const m = html.match(p);
    if (m) {
      const cost = parseFloat(m[1]);
      if (cost > 0 && cost < 50) return cost; // sanity check
    }
  }
  // If no shipping info found at all, assume free (Prime products)
  return 0;
}

// ── Map Amazon dimension key → eBay aspect name ─────────────────────────────────
function dimKeyToAspectName(key) {
  const MAP = {
    color_name:'Color', size_name:'Size', style_name:'Style', flavor_name:'Flavor',
    pattern_name:'Pattern', scent_name:'Scent', material_name:'Material', count_name:'Count',
    length_name:'Length', width_name:'Width', configuration_name:'Configuration',
    edition_name:'Edition', finish_name:'Finish', voltage_name:'Voltage',
  };
  return MAP[key] || key.replace('_name','').replace(/^\w/, c => c.toUpperCase());
}

// ── Extract price from buy-box section (more reliable than full HTML) ─────────────
function extractPriceFromBuyBox(html) {
  if (!html) return null;
  const core = html.match(/id="corePrice_feature_div"[\s\S]{0,2000}/)?.[0]
             || html.match(/id="apex_desktop"[\s\S]{0,3000}/)?.[0]
             || '';
  const pats = [
    /class="a-price-whole"[^>]*>\s*(\d[\d,]*)<\/span><span[^>]*class="a-price-fraction"[^>]*>\s*(\d+)/,
    /"priceAmount"\s*:\s*([\d.]+)/,
    /class="a-offscreen"[^>]*>\$([\d,]+\.?\d*)/,
    /"displayPrice"\s*:\s*"\$([\d,]+\.?\d*)"/,
  ];
  for (const section of [core, html]) {
    for (const p of pats) {
      const m = section.match(p);
      if (m) {
        const price = parseFloat(m[2] ? `${m[1].replace(/,/g,'')}.${m[2]}` : m[1].replace(/,/g,''));
        if (price > 0 && price < 10000) return price;
      }
    }
  }
  return null;
}

// ── Extract first hi-res image from an ASIN page ─────────────────────────────────
function extractMainImage(html) {
  // colorImages initial[0] is always the hero product shot
  const block = extractBlock(html, 'colorImages');
  if (block) {
    const m = block.match(/'initial'\s*:\s*\[\s*\{\s*(?:[^{}]*?)"hiRes"\s*:\s*"(https:[^"]+\.jpg)"/);
    if (m) return m[1];
  }
  const m = html.match(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/);
  return m ? m[1] : null;
}

// ── Bracket-counting block extractor ─────────────────────────────────────────
// Properly handles nested objects, strings with apostrophes, escape sequences
function extractBlock(html, searchStr) {
  const idx = html.indexOf(searchStr);
  if (idx === -1) return null;
  let i = idx + searchStr.length;
  while (i < html.length && html[i] !== '{' && html[i] !== '[') i++;
  if (i >= html.length) return null;
  const openChar = html[i], closeChar = openChar === '{' ? '}' : ']';
  let depth = 0, inStr = false, strChar = '', escaped = false;
  const start = i;
  for (; i < Math.min(html.length, start + 500000); i++) {
    const c = html[i];
    if (escaped) { escaped = false; continue; }
    if (c === '\\') { escaped = true; continue; }
    if (inStr) { if (c === strChar) inStr = false; continue; }
    if (c === '"' || c === "'") { inStr = true; strChar = c; continue; }
    if (c === openChar) depth++;
    else if (c === closeChar) { depth--; if (depth === 0) return html.slice(start, i + 1); }
  }
  return null;
}

// ── Extract color→image map from colorImages block ────────────────────────────
// Handles: 'Ivory': [...], "L'Special": [...], 'Brown-Checkered': [...]
// ── Extract color→image from swatch img elements in raw HTML ─────────────────
// Confirmed from live Amazon page: swatch imgs have alt="ColorName" src="...._SS64_.jpg"
function extractSwatchImages(html) {
  const map = {};
  const re = /alt="([^"]{2,60})"[^>]{0,200}src="(https:\/\/m\.media-amazon\.com\/images\/I\/[^"_]+\._SS\d+_\.jpg)"/g;
  const seen = new Set();
  let m;
  while ((m = re.exec(html)) !== null) {
    const name = m[1].trim();
    if (!seen.has(name) && name.length > 1 && !name.toLowerCase().includes('brand')) {
      seen.add(name);
      map[name] = m[2].replace(/\._SS\d+_\.jpg$/, '._SL1500_.jpg');
    }
  }
  console.log('[images] swatch map:', Object.keys(map).length, 'colors');
  return map;
}

// ── Extract colorToAsin + sizeToAsin from "colorToAsin" JSON ─────────────────
// Real format: {"Airy Blue 50\"x60\"": {"asin": "B0XXXXX"}, ...}
function extractColorAsinMaps(html) {
  const colorToAsin = {};
  const sizeToAsin  = {};
  const block = extractBlock(html, '"colorToAsin"');
  if (!block) return { colorToAsin, sizeToAsin };
  let data = {};
  try { data = JSON.parse(block); } catch { return { colorToAsin, sizeToAsin }; }
  for (const [key, val] of Object.entries(data)) {
    const asin = val?.asin || (typeof val === 'string' ? val : null);
    if (!asin || asin.length !== 10) continue;
    // Keys: "Color 50\"x60\"" — literal backslash-quotes. After JSON.parse still have \\"
    const cleaned = key.replace(/\\"/g, '"').replace(/\\'/g, "'");
    // Match: ColorName + space + dimensions like 50"x60" or 60"x80"
    const sizeM = cleaned.match(/^(.+?)\s+(\d+(?:\.\d+)?"[×x]\d+(?:\.\d+)?"?)\s*$/i);
    if (sizeM) {
      const color = sizeM[1].trim();
      const size  = sizeM[2].trim();
      if (!colorToAsin[color]) colorToAsin[color] = asin;
      if (!sizeToAsin[size])   sizeToAsin[size]   = asin;
    } else {
      if (!colorToAsin[cleaned]) colorToAsin[cleaned] = asin;
    }
  }
  console.log('[colorToAsin] colors:', Object.keys(colorToAsin).length, 'sizes:', Object.keys(sizeToAsin).length);
  return { colorToAsin, sizeToAsin };
}

// ── eBay Taxonomy API: get leaf category suggestions ─────────────────────────
async function getCategories(title, token, sandbox=false) {
  const EBAY_API = getEbayUrls(sandbox).EBAY_API;
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
  "title": "<see title rules below>",
  "aspects": {
    "Brand": ["Unbranded"],
    "Color": ["See Listing"],
    "Material": ["<actual material>"],
    "Size": ["See Listing"]
  }
}

TITLE RULES - follow strictly:
- Exactly 10-12 words, no more no less
- Remove ALL brand/seller/store names (L'AGRATY, Woman Within, Amazon Basics, etc)
- Lead with product type then key descriptors: material, style, use case
- No special chars, colons, quotes, or dimensions in the title
- No ALL CAPS words, no filler words (perfect, great, best)
- NEVER use: authentic, genuine, original, real, verified, certified — eBay bans these
- Title Case formatting
- Good example: "Chunky Knit Throw Blanket Soft Chenille Handmade Couch Home Decor"
- Bad example: "L'AGRATY Authentic Chunky Knit Blanket 30x40 Chenille..."

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
async function resolvePolicies(token, supplied, sandbox=false) {
  const EBAY_API = getEbayUrls(sandbox).EBAY_API;
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
    // Try to create a fulfillment policy with multiple fallback service codes
    // eBay accounts have different supported services depending on registration type
    const serviceAttempts = [
      { shippingServiceCode: 'ShippingMethodStandard', freeShipping: true },
      { shippingServiceCode: 'USPSFirstClass', freeShipping: true },
      { shippingServiceCode: 'USPSPriority', freeShipping: true },
    ];
    for (const svc of serviceAttempts) {
      const r = await fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy`, {
        method: 'POST', headers: auth,
        body: JSON.stringify({
          name: 'DropSync Free Shipping', marketplaceId: 'EBAY_US',
          categoryTypes: [{ name: 'ALL_EXCLUDING_MOTORS_VEHICLES', default: true }],
          handlingTime: { value: 3, unit: 'DAY' },
          shippingOptions: [{ optionType: 'DOMESTIC', costType: 'FLAT_RATE',
            shippingServices: [{
              shippingServiceCode: svc.shippingServiceCode,
              freeShipping: svc.freeShipping,
              shippingCost: { value: '0.00', currency: 'USD' },
              buyerResponsibleForShipping: false,
              sortOrder: 1,
            }],
          }],
        }),
      });
      const d = await r.json();
      if (d.fulfillmentPolicyId) { p.fulfillmentPolicyId = d.fulfillmentPolicyId; break; }
      // If name already exists, fetch it
      if ((d.errors||[]).some(e => e.errorId === 20400)) {
        const existing = await fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy?marketplace_id=EBAY_US`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
        p.fulfillmentPolicyId = (existing.fulfillmentPolicies||[]).find(x=>x.name==='DropSync Free Shipping')?.fulfillmentPolicyId
                              || (existing.fulfillmentPolicies||[])[0]?.fulfillmentPolicyId || '';
        if (p.fulfillmentPolicyId) break;
      }
      console.warn('[policy] create failed with', svc.shippingServiceCode, ':', JSON.stringify(d).slice(0,200));
    }
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
    const d = await r.json();
    p.returnPolicyId = d.returnPolicyId || '';
    // If name already exists, fetch it
    if (!p.returnPolicyId && (d.errors||[]).some(e => e.errorId === 20400)) {
      const existing = await fetch(`${EBAY_API}/sell/account/v1/return_policy?marketplace_id=EBAY_US`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
      p.returnPolicyId = (existing.returnPolicies||[]).find(x=>x.name==='DropSync 30-Day Returns')?.returnPolicyId
                       || (existing.returnPolicies||[])[0]?.returnPolicyId || '';
    }
  }

  if (!p.fulfillmentPolicyId) throw new Error(
    'No shipping policy available. ' +
    'Go to eBay Seller Hub → Account → Business Policies → Create a Shipping Policy, ' +
    'then select it in DropSync Settings.'
  );
  if (!p.returnPolicyId) throw new Error(
    'No return policy available. ' +
    'Go to eBay Seller Hub → Account → Business Policies → Create a Return Policy, ' +
    'then select it in DropSync Settings.'
  );
  console.log(`[policies] resolved fp=${p.fulfillmentPolicyId} pp=${p.paymentPolicyId} rp=${p.returnPolicyId}`);
  return p;
}

// ── Ensure merchant location exists ──────────────────────────────────────────
async function ensureLocation(auth, sandbox=false) {
  const EBAY_API = getEbayUrls(sandbox).EBAY_API;
  const r = await fetch(`${EBAY_API}/sell/inventory/v1/location`, { headers: auth }).then(r => r.json()).catch(() => ({}));
  if ((r.locations || []).length) {
    const key = r.locations[0].merchantLocationKey;
    console.log(`[location] found existing: ${key}`);
    return key;
  }
  const key = 'MainWarehouse';
  const cr = await fetch(`${EBAY_API}/sell/inventory/v1/location/${key}`, {
    method: 'POST', headers: auth,
    body: JSON.stringify({
      location: { address: { addressLine1: '123 Main St', city: 'San Jose', stateOrProvince: 'CA', postalCode: '95125', country: 'US' } },
      locationTypes: ['WAREHOUSE'],
      name: key,
      merchantLocationStatus: 'ENABLED',
    }),
  });
  const cd = await cr.json().catch(() => ({}));
  console.log(`[location] create result: ${cr.status} ${JSON.stringify(cd).slice(0,200)}`);
  // Wait for eBay to propagate the new location before using it
  await sleep(3000);
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

// ── Shared Amazon scraper — called by both scrape and revise actions ───────────
// Returns the full product object (same shape as scrape action), or null on failure.
async function scrapeAmazonProduct(inputUrl, preloadedHtml = null) {
  let url = (inputUrl || '').trim();
  if (!url) return null;

  // Normalize to clean dp/ASIN URL
  const asinM = url.match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})/);
  const asinP = url.match(/[?&]asin=([A-Z0-9]{10})/i);
  const asin  = asinM?.[1] || asinP?.[1];
  if (asin) url = `https://www.amazon.com/dp/${asin}?th=1`;

  const ua = randUA();
  // Use pre-fetched HTML if provided by browser (bypasses Vercel IP blocking)
  let html = preloadedHtml || null;
  if (!html) {
    html = await fetchPage(url, ua);
    if (!html && asin) { await sleep(1500); html = await fetchPage(`https://www.amazon.com/dp/${asin}?psc=1`, ua); }
    if (!html && asin) { await sleep(2000); html = await fetchPage(`https://www.amazon.com/product/dp/${asin}`, ua); }
  } else {
    console.log('[scrapeAmazonProduct] using client-provided HTML — skipping server-side fetch');
  }
  if (!html) return null;

  const product = {
    url, source: 'amazon', asin: asin || '',
    title: '', price: 0, images: [],
    description: '', aspects: {}, breadcrumbs: [],
    variations: [], variationImages: {}, hasVariations: false,
    inStock: true, quantity: 1,
    bullets: [], descriptionPara: '',
    comboAsin: {}, sizePrices: {}, comboPrices: {},
  };

  // Title
  const tM = html.match(/id="productTitle"[^>]*>\s*([^<]{5,})/);
  if (tM) product.title = tM[1].trim().replace(/\s+/g,' ');

  // Breadcrumbs
  const bcRaw = [...html.matchAll(/class="a-link-normal"[^>]*>\s*([^<]{2,40})\s*<\/a>/g)];
  product.breadcrumbs = bcRaw.slice(0, 6).map(m => m[1].trim()).filter(s => s.length > 1 && !/^\d+$/.test(s));

  // Price — use buy-box section for accuracy
  product.price = extractPriceFromBuyBox(html) || extractPrice(html) || 0;
  // Stock — buy-box only to avoid false OOS from ads/recommendations
  const _bbMain1 = (html.match(/id="availability"[\s\S]{0,3000}/)?.[0]||'')
                 + (html.match(/id="addToCart_feature_div"[\s\S]{0,1000}/)?.[0]||'');
  if (_bbMain1.length > 30) {
    product.inStock = !_bbMain1.toLowerCase().includes('currently unavailable');
  } else {
    // No buy-box found — check for strong signals
    product.inStock = html.includes('id="add-to-cart-button"')
                   || (!html.includes('id="outOfStock"') && !html.includes('Currently unavailable'));
  }

  // Images
  const imgs = [...html.matchAll(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/g)]
    .map(m => m[1]).filter((v,i,a) => a.indexOf(v)===i);
  product.images = imgs.slice(0, 12);

  // Bullets
  const extractBullets = (h) => {
    const results = [];
    const sectionPatterns = [
      /id="productFactsDesktopExpander"[^>]*>([\s\S]{100,8000}?)<\/div>\s*<\/div>\s*<\/div>/,
      /id="featurebullets_feature_div"[^>]*>([\s\S]{100,6000}?)<\/div>\s*<\/div>/,
      /id="feature-bullets"[^>]*>([\s\S]{100,6000}?)<\/div>\s*<\/div>/,
    ];
    let sectionHtml = '';
    for (const pat of sectionPatterns) { const m = h.match(pat); if (m) { sectionHtml = m[1]; break; } }
    if (sectionHtml) {
      for (const m of [...sectionHtml.matchAll(/<li[^>]*>[\s\S]*?<span[^>]*class="[^"]*a-list-item[^"]*"[^>]*>([\s\S]{15,600}?)<\/span>/g)]) {
        const text = m[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&#\d+;/g,'').trim();
        if (text.length > 15 && !text.includes('Your Orders')) results.push(text);
      }
    }
    if (!results.length) {
      for (const m of [...h.matchAll(/<li[^>]*>\s*<span[^>]*a-list-item[^"]*"[^>]*>([\s\S]{20,600}?)<\/span>/g)]) {
        const text = m[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').trim();
        if (text.length > 20 && !text.includes('Your Orders') && !text.includes('Drop off')) { results.push(text); }
        if (results.length >= 8) break;
      }
    }
    return results.slice(0, 8);
  };
  product.bullets = extractBullets(html);

  const descParaM = html.match(/id="productDescription"[^>]*>[\s\S]{0,200}<p[^>]*>([\s\S]{30,1000}?)<\/p>/);
  product.descriptionPara = descParaM ? descParaM[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').trim() : '';

  // Build eBay description
  const buildEbayDesc = (title, buls, para, aspects) => {
    const bulletHtml = buls.length ? '<ul>' + buls.map(b => `<li>${b.replace(/</g,'&lt;').replace(/>/g,'&gt;')}</li>`).join('') + '</ul>' : '';
    const specRows = Object.entries(aspects||{}).filter(([k,v])=>!['ASIN','UPC','Color','Size','Brand Name','Brand'].includes(k)&&v[0]&&v[0].length<80).slice(0,10).map(([k,v])=>`<tr><td><b>${k}</b></td><td>${v[0]}</td></tr>`).join('');
    const specsTable = specRows ? `<br/><table border="0" cellpadding="4" cellspacing="0" width="100%"><tbody>${specRows}</tbody></table>` : '';
    return [`<h2>${title}</h2>`,bulletHtml,para?`<p>${para}</p>`:'',specsTable,'<br/><p style="font-size:11px;color:#888">Ships from US. Item is new. Please message us with any questions before purchasing.</p>'].filter(Boolean).join('\n');
  };
  product.description = buildEbayDesc(product.title, product.bullets, product.descriptionPara, product.aspects) || product.title;

  // Item specifics
  for (const [,k,v] of [...html.matchAll(/<th[^>]*class="[^"]*prodDetSectionEntry[^"]*"[^>]*>([^<]+)<\/th>\s*<td[^>]*>([^<]+)<\/td>/gi)]) {
    const key = k.trim(), val = v.trim().replace(/\s+/g,' ');
    if (key && val && val.length < 120 && !val.includes('›')) product.aspects[key] = [val];
  }
  for (const [,k,v] of [...html.matchAll(/(Brand|Material|Color|Size|Style|Weight|Dimensions?)\s*:\s*([^\n<]{2,60})/g)]) {
    if (!product.aspects[k]) product.aspects[k] = [v.trim()];
  }

  // Variations
  // ── Variation parsing — handles ALL Amazon listing types ─────────────────────
  const swatchImgMap = extractSwatchImages(html);

  // Detect ALL variation dimensions from variationValues (not just color+size)
  let varVals = null;
  const vvM = html.match(/"variationValues"\s*:\s*(\{(?:[^{}]|\{[^{}]*\})*\})/);
  if (vvM) try { varVals = JSON.parse(vvM[1]); } catch {}

  // Build dimension list: any *_name key with values
  const allDims = [];
  if (varVals) {
    for (const [key, vals] of Object.entries(varVals)) {
      if (Array.isArray(vals) && vals.length) allDims.push({ key, name: dimKeyToAspectName(key), values: vals });
    }
  }
  const hasVar = allDims.length > 0;
  product.hasVariations = hasVar;

  if (hasVar) {
    // Skip any dimension with only 1 unique value (e.g. Color="Purple" only)
    // — it adds no meaningful variation and wastes one of eBay's 2 dimension slots
    const meaningfulDims = allDims.filter(d => d.values.length > 1);
    // If all dims have 1 value (edge case), fall back to all dims
    const effectiveDims = meaningfulDims.length > 0 ? meaningfulDims : allDims;

    // Primary dimension = Color/Colour if present among meaningful dims, else first
    const primaryDim   = effectiveDims.find(d => /color|colour/i.test(d.name)) || effectiveDims[0];
    const secondaryDim = effectiveDims.find(d => d !== primaryDim) || null;
    const primaryVals   = primaryDim.values;
    const secondaryVals = secondaryDim?.values || [];

    // Log skipped single-value dimensions for debugging
    const skippedDims = allDims.filter(d => d.values.length === 1);
    if (skippedDims.length) console.log(`[scraper] skipping single-value dims: ${skippedDims.map(d=>d.name+'='+d.values[0]).join(', ')}`);

    // Parse dimension order from page
    let dimOrder = null;
    const dimM = html.match(/"dimensions"\s*:\s*(\[[^\]]{0,400}\])/s);
    if (dimM) try { dimOrder = JSON.parse(dimM[1]); } catch {}
    if (!dimOrder || !dimOrder.length) dimOrder = allDims.map(d => d.key);

    // Build comboAsin map from dimensionToAsinMap
    const dtaBlock = extractBlock(html, '"dimensionToAsinMap"');
    let dtaMap = {};
    try { dtaMap = JSON.parse(dtaBlock); } catch {}

    const pIdx = dimOrder.indexOf(primaryDim.key)   >= 0 ? dimOrder.indexOf(primaryDim.key)   : 0;
    const sIdx = secondaryDim && dimOrder.indexOf(secondaryDim.key) >= 0
               ? dimOrder.indexOf(secondaryDim.key)
               : (pIdx === 0 ? 1 : 0);

    const comboAsin = {};      // "PrimaryVal|SecondaryVal" → ASIN
    const primaryToAsins = {}; // primaryVal → [ASINs]
    for (const [code, asin] of Object.entries(dtaMap)) {
      const parts = code.split('_').map(Number);
      const pVal  = primaryVals[parts[pIdx]];
      const sVal  = secondaryDim ? (secondaryVals[parts[sIdx]] ?? '') : '';
      if (!pVal) continue;
      const key = `${pVal}|${sVal}`;
      comboAsin[key] = asin;
      if (!primaryToAsins[pVal]) primaryToAsins[pVal] = [];
      if (!primaryToAsins[pVal].includes(asin)) primaryToAsins[pVal].push(asin);
    }

    // Fallback: extract from color→ASIN patterns in HTML (handles some edge cases)
    if (!Object.keys(comboAsin).length) {
      const { colorToAsin: ctaMap } = extractColorAsinMaps(html);
      for (const [c, asin] of Object.entries(ctaMap)) {
        comboAsin[`${c}|`] = asin;
        primaryToAsins[c] = [asin];
      }
    }

    // ── Stock + Price per combo ───────────────────────────────────────────────
    // Strategy:
    //   1. Get baseInStock from main page buy-box (reliable, current selection)
    //   2. Fetch per-ASIN pages for price + stock
    //   3. For stock: ONLY mark OOS if buy-box EXPLICITLY says "currently unavailable"
    //      If blocked/ambiguous → fall back to baseInStock (main page result)
    //   4. This avoids false OOS from Vercel's server location + ads on page

    // baseInStock: what the main page currently shows for the selected variant
    const bbMain = (html.match(/id="availability"[\s\S]{0,3000}/)?.[0] || '')
                 + (html.match(/id="addToCart_feature_div"[\s\S]{0,1000}/)?.[0] || '');
    const mainInStock = bbMain.length > 30
      ? !bbMain.toLowerCase().includes('currently unavailable')
      : !html.includes('id="outOfStock"') && !html.includes('Currently unavailable');

    // Pick a "fullDim" primary value to fetch per-secondary prices from
    // Prefer the one with most secondary values mapped (likely full coverage)
    const fullPrimary = Object.entries(primaryToAsins)
      .sort((a,b) => b[1].length - a[1].length)[0]?.[0] || primaryVals[0];

    // Build what to fetch for price+stock:
    // - If 2 dimensions: fetch each secondary value's ASIN via fullPrimary (e.g. one color × all sizes)
    // - If 1 dimension only (Style, Size, Flavor etc.): fetch EACH primary value's ASIN
    const secToFetchAsin = {};  // key → asin  (key = secondaryVal or primaryVal for 1-dim)
    if (secondaryDim) {
      // 2 dimensions: fetch via fullPrimary's secondary ASINs
      for (const [key, asin] of Object.entries(comboAsin)) {
        const [pv, sv] = key.split('|');
        if (pv === fullPrimary) secToFetchAsin[sv] = asin;
      }
      // Fallback: if fullPrimary has no combos mapped, use any
      if (!Object.keys(secToFetchAsin).length && Object.keys(comboAsin).length) {
        secToFetchAsin[''] = Object.values(comboAsin)[0];
      }
    } else {
      // 1 dimension only: fetch EACH primary value's ASIN for individual price+stock
      for (const [key, asin] of Object.entries(comboAsin)) {
        const pv = key.split('|')[0];
        secToFetchAsin[pv] = asin;  // key = primaryVal (not secondary)
      }
    }

    const asinInStock = {};  // ASIN → true/false/undefined
    const asinPrice   = {};  // ASIN → price

    // Fetch per-secondary-value pages for price + stock
    await Promise.all(Object.entries(secToFetchAsin).map(async ([sVal, asin]) => {
      if (!asin) return;
      const h = await fetchPage(`https://www.amazon.com/dp/${asin}`, ua);
      if (!h) return; // blocked → leave undefined (will use mainInStock)
      const price = extractPriceFromBuyBox(h);
      if (price) asinPrice[asin] = price;
      const bb = (h.match(/id="availability"[\s\S]{0,3000}/)?.[0] || '')
               + (h.match(/id="addToCart_feature_div"[\s\S]{0,1000}/)?.[0] || '');
      if (bb.length > 30) {
        asinInStock[asin] = !bb.toLowerCase().includes('currently unavailable');
      }
      // If bb is short/empty: leave undefined → will use mainInStock fallback
    }));

    // Build per-secondary stock + price maps
    const secInStock = {};  // secondaryVal → true/false
    const sizePrices = {};  // secondaryVal → price
    for (const [sVal, asin] of Object.entries(secToFetchAsin)) {
      if (asinInStock[asin] !== undefined) secInStock[sVal] = asinInStock[asin];
      if (asinPrice[asin]) sizePrices[sVal] = asinPrice[asin];
    }

    // Fetch remaining unique ASINs for per-combo pricing (up to 15 total)
    // This handles products where prices vary by BOTH dimensions (e.g. Size × Style)
    const fetchedAsins = new Set(Object.values(secToFetchAsin));
    const remainingAsins = [...new Set(Object.values(comboAsin))]
      .filter(a => a && !fetchedAsins.has(a) && !asinPrice[a])
      .slice(0, 15); // cap to avoid Vercel timeout
    if (remainingAsins.length) {
      console.log(`[scraper] fetching ${remainingAsins.length} additional ASINs for per-combo pricing`);
      for (let i = 0; i < remainingAsins.length; i += 5) {
        await Promise.all(remainingAsins.slice(i, i + 5).map(async a => {
          const h = await fetchPage(`https://www.amazon.com/dp/${a}`, ua);
          if (!h) return;
          const price = extractPriceFromBuyBox(h);
          if (price) asinPrice[a] = price;
          const bb = (h.match(/id="availability"[\s\S]{0,3000}/)?.[0] || '')
                   + (h.match(/id="addToCart_feature_div"[\s\S]{0,1000}/)?.[0] || '');
          if (bb.length > 30) asinInStock[a] = !bb.toLowerCase().includes('currently unavailable');
        }));
        if (i + 5 < remainingAsins.length) await sleep(300);
      }
    }

    // Fill missing prices with main page price
    const mainPrice = extractPriceFromBuyBox(html) || product.price || 0;
    if (secondaryDim) {
      // 2-dim: fill missing secondary prices
      for (const sv of secondaryVals) { if (!sizePrices[sv]) sizePrices[sv] = mainPrice; }
    } else {
      // 1-dim: sizePrices keys by primary val — fill any missing
      for (const pv of primaryVals) { if (!sizePrices[pv]) sizePrices[pv] = mainPrice; }
    }

    // Build comboInStock + comboPrices
    const comboInStock = {};
    const comboPrices  = {};
    for (const [key, asin] of Object.entries(comboAsin)) {
      const [, sv] = key.split('|');
      // Price: per-ASIN → per-secondary → main page
      comboPrices[key] = asinPrice[asin] || sizePrices[sv] || mainPrice;
      // Stock: explicit per-ASIN → per-secondary level → mainInStock fallback
      if (asinInStock[asin] !== undefined) {
        comboInStock[key] = asinInStock[asin];
      } else if (secInStock[sv] !== undefined) {
        comboInStock[key] = secInStock[sv];
      } else {
        // Unknown — default to whatever the main page shows
        comboInStock[key] = mainInStock;
      }
    }

    // product.inStock = true if ANY combo is in stock
    product.inStock = Object.keys(comboInStock).length > 0
      ? Object.values(comboInStock).some(v => v !== false)
      : mainInStock;

    // Best price = cheapest in-stock secondary value
    const inStockPrices = Object.entries(comboPrices)
      .filter(([k]) => comboInStock[k] !== false)
      .map(([,p]) => p)
      .filter(p => p > 0);
    if (inStockPrices.length) product.price = Math.min(...inStockPrices);
    else { const allPrices = Object.values(comboPrices).filter(p=>p>0); if (allPrices.length) product.price = Math.min(...allPrices); }

    // Detect if per-variant price fetches were blocked (all prices identical)
    // This happens when Amazon redirects all per-ASIN server requests to the same page
    const allComboPrices = Object.values(comboPrices).filter(p => p > 0);
    const uniquePrices = [...new Set(allComboPrices)];
    if (allComboPrices.length > 1 && uniquePrices.length === 1) {
      // All variants returned same price — server-side fetches were blocked/redirected
      product._pricesFailed = true;
      product._pricesFailedReason = 'All variant prices identical — Amazon blocked per-ASIN server fetches. Cannot verify individual prices.';
      console.warn(`[scraper] price fetch failed — all ${allComboPrices.length} combos returned $${uniquePrices[0]}`);
    } else {
      product._pricesFailed = false;
    }

    // ── Per-color images ──────────────────────────────────────────────────────
    const colorImgMap = {};
    for (const pv of primaryVals) colorImgMap[pv] = swatchImgMap[pv] || '';
    // Fetch images for primaries that don't have swatch images
    const needImg = primaryVals.filter(pv => !colorImgMap[pv]);
    if (needImg.length) {
      await Promise.all(needImg.slice(0, 8).map(async pv => {
        const asin = primaryToAsins[pv]?.[0];
        if (!asin) return;
        const h = await fetchPage(`https://www.amazon.com/dp/${asin}`, ua);
        if (h) { const img = extractMainImage(h); if (img) colorImgMap[pv] = img; }
      }));
    }
    // Add color images to product.images
    for (const pv of primaryVals) {
      const img = colorImgMap[pv];
      if (img && !product.images.includes(img)) product.images.push(img);
    }
    // Fallback: assign from product.images for any still-missing
    let fi = 0;
    for (const pv of primaryVals) {
      if (!colorImgMap[pv]) colorImgMap[pv] = product.images[fi++ % Math.max(1, product.images.length)] || '';
    }

    // ── Build variation groups ────────────────────────────────────────────────
    // Primary dimension group (e.g. Color)
    product.variations.push({
      name: primaryDim.name,
      values: primaryVals.map(pv => {
        const inStock = Object.entries(comboInStock)
          .some(([k,v]) => k.startsWith(`${pv}|`) && v !== false);
        const pvPrices = Object.entries(comboPrices)
          .filter(([k]) => k.startsWith(`${pv}|`)).map(([,p]) => p).filter(p=>p>0);
        return {
          value:   pv,
          price:   pvPrices.length ? Math.min(...pvPrices) : mainPrice,
          image:   colorImgMap[pv] || '',
          inStock: inStock,
          enabled: inStock,
        };
      }),
    });
    // Secondary dimension group (e.g. Size, Style)
    if (secondaryDim) {
      product.variations.push({
        name: secondaryDim.name,
        values: secondaryVals.map(sv => {
          const inStock = Object.entries(comboInStock)
            .some(([k,v]) => k.endsWith(`|${sv}`) && v !== false);
          return {
            value:   sv,
            price:   sizePrices[sv] || mainPrice,
            inStock: inStock,
            enabled: inStock,
            image:   '',
          };
        }),
      });
    }

    // Store on product
    product.comboAsin     = comboAsin;
    product.comboInStock  = comboInStock;
    product.sizePrices    = sizePrices;
    product.comboPrices   = comboPrices;
    product.variationImages[primaryDim.name] = Object.fromEntries(
      primaryVals.map(pv => [pv, colorImgMap[pv]]).filter(([,img]) => img)
    );
    // Store primary/secondary dim names for push/revise to use
    product._primaryDimName   = primaryDim.name;
    product._secondaryDimName = secondaryDim?.name || null;
  }

  console.log(`[scrapeAmazonProduct] "${product.title?.slice(0,50)}" price=$${product.price} imgs=${product.images.length} hasVar=${product.hasVariations}`);
  return product;
}

// ══════════════════════════════════════════════════════════════════════════════
// ═══════════════════════════════════════════════════════════════════════════════
// PUSH + REVISE — Clean rewrite
// ═══════════════════════════════════════════════════════════════════════════════
//
// PUSH:   Creates a brand-new eBay listing from a scraped Amazon product.
// REVISE: Updates an existing eBay listing in-place (keeps listing ID live).
//
// Handles ALL Amazon listing types:
//   • Simple (no variations)
//   • Color only
//   • Size only
//   • Color + Size
//   • Any other dimension (treated as "size")
//
// Stock logic:
//   If comboAsin map exists → check comboInStock per combo → qty=0 if OOS
//   If no comboAsin          → trust product.inStock for simple / defaultQty for all variants
//
// Image logic:
//   Per-SKU inventory item: [this_color_image]  (just one — eBay shows the right swatch)
//   Group imageUrls:        all product images up to 12
//   If no color image:      use product.images[0]
//
// Price formula: (amazonCost + handling) × (1 + markup/100) / (1 − 0.1335) + 0.30
// ═══════════════════════════════════════════════════════════════════════════════

// ─── Shared helpers ───────────────────────────────────────────────────────────

function makeApplyMk(markupPct, handling) {
  const fee = 0.1335;
  return function applyMk(cost) {
    const c = parseFloat(cost) || 0;
    if (c <= 0) return 0;
    return Math.max(
      Math.ceil(((c + handling) * (1 + markupPct / 100) / (1 - fee) + 0.30) * 100) / 100,
      0.99
    );
  };
}

function buildDescription(title, bullets, para, aspects) {
  const bulletHtml = (bullets || []).length
    ? '<ul>' + bullets.map(b => `<li>${String(b).replace(/</g,'&lt;').replace(/>/g,'&gt;')}</li>`).join('') + '</ul>'
    : '';
  const specRows = Object.entries(aspects || {})
    .filter(([k, v]) => !['ASIN','UPC','Color','Size','Brand Name','Brand'].includes(k) && v?.[0] && String(v[0]).length < 80)
    .slice(0, 10)
    .map(([k, v]) => `<tr><td><b>${k}</b></td><td>${v[0]}</td></tr>`).join('');
  const specsTable = specRows
    ? `<br/><table border="0" cellpadding="4" cellspacing="0" width="100%"><tbody>${specRows}</tbody></table>`
    : '';
  return [
    `<h2>${title}</h2>`,
    bulletHtml,
    para ? `<p>${para}</p>` : '',
    specsTable,
    '<br/><p style="font-size:11px;color:#888">Ships from US. Item is new. Please message us with any questions before purchasing.</p>',
  ].filter(Boolean).join('\n');
}

// Build a flat variant list from product variations
// Returns array of { sku, dimKey (e.g. "Purple|Queen"), dims {}, price, qty, image }
function buildVariants({ product, groupSku, applyMk, defaultQty, body }) {
  const SKU_MAX  = 50;
  const prefix   = groupSku + '-';
  const maxSuffix = SKU_MAX - prefix.length;

  function mkSku(parts) {
    const raw = parts.join('_').replace(/[^A-Z0-9]/gi, '_').toUpperCase().replace(/_+/g, '_').replace(/^_|_$/g, '');
    if (raw.length <= maxSuffix) return prefix + raw;
    const hash = raw.split('').reduce((h, c) => ((h << 5) - h + c.charCodeAt(0)) | 0, 0);
    const hashStr = (hash >>> 0).toString(16).toUpperCase().padStart(8, '0');
    return prefix + raw.slice(0, maxSuffix - 9) + '_' + hashStr;
  }

  const comboAsin    = product.comboAsin    || {};
  const comboInStock = product.comboInStock || {};
  const comboPrices  = product.comboPrices  || {};
  const sizePrices   = product.sizePrices   || {};
  const basePrice    = parseFloat(product.price || product.cost || 0);
  const hasComboData = Object.keys(comboAsin).length > 0;

  // Use _primaryDimName/_secondaryDimName if available (set by new scraper)
  // Fall back to detecting by name for backward compat with cached products
  const primaryName   = product._primaryDimName   || null;
  const secondaryName = product._secondaryDimName || null;
  const primaryGroup  = primaryName
    ? product.variations?.find(v => v.name === primaryName)
    : product.variations?.find(v => /color|colour/i.test(v.name)) || product.variations?.[0];
  const secondaryGroup = secondaryName
    ? product.variations?.find(v => v.name === secondaryName)
    : product.variations?.find(v => v !== primaryGroup);

  // Image map: keyed by primary dimension value
  const imgMap = Object.assign(
    {},
    product.variationImages?.['Color'] || {},
    primaryGroup ? (product.variationImages?.[primaryGroup.name] || {}) : {}
  );

  function getAmazonPrice(pVal, sVal) {
    const key = `${pVal || ''}|${sVal || ''}`;
    return comboPrices[key]
      || sizePrices[sVal || '']
      || sizePrices[pVal || '']
      || basePrice || 0;
  }

  function isInStock(pVal, sVal) {
    if (!hasComboData) return true; // no combo data → assume in stock (safer than OOS)
    const key = `${pVal || ''}|${sVal || ''}`;
    if (!comboAsin[key]) return false;           // combo doesn't exist on Amazon
    if (comboInStock[key] === false) return false; // explicitly OOS
    return true; // comboInStock = true or undefined → in stock
  }

  function getImage(pVal) {
    if (!pVal) return product.images?.[0] || '';
    return imgMap[pVal]
      || Object.entries(imgMap).find(([k]) => k.toLowerCase() === (pVal||'').toLowerCase())?.[1]
      || product.images?.[0] || '';
  }

  const variants = [];

  if (primaryGroup && secondaryGroup) {
    // Primary × Secondary (e.g. Color × Size, Color × Style)
    for (const pv of primaryGroup.values) {
      for (const sv of secondaryGroup.values) {
        const inStock = isInStock(pv.value, sv.value);
        const ebayPrice = applyMk(getAmazonPrice(pv.value, sv.value));
        variants.push({
          sku:    mkSku([pv.value, sv.value]),
          dims:   { [primaryGroup.name]: pv.value, [secondaryGroup.name]: sv.value },
          dimKey: `${pv.value}|${sv.value}`,
          price:  (ebayPrice > 0 ? ebayPrice : parseFloat(product.myPrice || 9.99)).toFixed(2),
          qty:    inStock ? defaultQty : 0,
          image:  getImage(pv.value),
        });
      }
    }
  } else if (primaryGroup) {
    // Single dimension
    for (const pv of primaryGroup.values) {
      const inStock = isInStock(pv.value, '');
      const ebayPrice = applyMk(getAmazonPrice(pv.value, ''));
      variants.push({
        sku:    mkSku([pv.value]),
        dims:   { [primaryGroup.name]: pv.value },
        dimKey: `${pv.value}|`,
        price:  (ebayPrice > 0 ? ebayPrice : parseFloat(product.myPrice || 9.99)).toFixed(2),
        qty:    inStock ? defaultQty : 0,
        image:  getImage(pv.value),
      });
    }
  }

  // eBay allows max 250 variants
  return variants.slice(0, 250);
}

// Build the group variesBy spec — must list ALL values for each variation dimension
function buildVariesBy(product, primaryGroup, secondaryGroup) {
  const specs = [];
  if (primaryGroup) {
    specs.push({ name: primaryGroup.name, values: primaryGroup.values.map(v => v.value) });
  }
  if (secondaryGroup) {
    specs.push({ name: secondaryGroup.name, values: secondaryGroup.values.map(v => v.value) });
  }
  // aspectsImageVariesBy: the dimension that has per-variant images
  // Use primary if it's color-like, otherwise empty (all variants share same images)
  const isColorPrimary = primaryGroup && /color|colour/i.test(primaryGroup.name);
  return {
    aspectsImageVariesBy: isColorPrimary ? [primaryGroup.name] : [],
    specifications: specs,
  };
}

// ─── PUSH action ──────────────────────────────────────────────────────────────

async function handlePush({ body, res, resolvePolicies, getCategories, aiEnrich, sanitizeTitle, ensureLocation, buildOffer, sleep, getEbayUrls }) {
  const sandbox  = body.sandbox === true || body.sandbox === 'true';
  const EBAY_API = getEbayUrls(sandbox).EBAY_API;
  const auth     = { Authorization: `Bearer ${body.access_token}`, 'Content-Type': 'application/json', 'Content-Language': 'en-US', 'Accept-Language': 'en-US' };
  const { access_token, product, fulfillmentPolicyId, paymentPolicyId, returnPolicyId } = body;
  if (!access_token || !product) return res.status(400).json({ error: 'Missing access_token or product' });

  // Guards
  // FIX: validate title — empty title means Amazon was blocked during scrape
  if (!product.title || product.title.trim().length < 3)
    return res.status(400).json({ error: 'Missing title — re-import this product from the Import tab first.' });
  if (!product.images?.length)
    return res.status(400).json({ error: 'No images — re-import from the Import tab first.' });
  if (!product.hasVariations && !product.price && !product.cost && !product.myPrice)
    return res.status(400).json({ error: 'No price — re-import from the Import tab first.' });

  const markupPct      = parseFloat(body.markup  ?? 0);
  const handling       = parseFloat(body.handlingCost ?? 2);
  const defaultQty     = parseInt(body.quantity || product.quantity) || 1;
  const amazonShipping = parseFloat(product.shippingCost || 0); // paid shipping on Amazon
  // Price formula: (amazonCost + amazonShipping + handling) × markup ÷ (1−fee) + $0.30
  const applyMk = (cost) => {
    const c = parseFloat(cost) || 0;
    if (c <= 0) return 0;
    return Math.max(
      Math.ceil(((c + amazonShipping + handling) * (1 + markupPct / 100) / (1 - 0.1335) + 0.30) * 100) / 100,
      0.99
    );
  };

  console.log(`[push] "${product.title?.slice(0,60)}" hasVar=${product.hasVariations} imgs=${product.images?.length} markup=${markupPct}%`);

  // Policies
  let policies;
  try { policies = await resolvePolicies(access_token, { fulfillmentPolicyId, paymentPolicyId, returnPolicyId }, false); }
  catch(e) { return res.status(400).json({ error: e.message }); }

  // AI enrichment
  const suggestions  = await getCategories(product.title || '', access_token).catch(() => []);
  const ai           = await aiEnrich(product.title, product.breadcrumbs || [], product.aspects || {}, suggestions).catch(() => null);
  const categoryId   = ai?.categoryId || suggestions[0]?.id || '11450';
  const listingTitle = sanitizeTitle(product.ebayTitle || ai?.title || product.title || 'Product');

  // Description
  const ebayDescription = buildDescription(listingTitle, product.bullets || [], product.descriptionPara || '', product.aspects || {})
    || product.description || listingTitle;

  // Base aspects (strip Color/Size — variants carry their own)
  const aspects = { ...(product.aspects || {}), ...(ai?.aspects || {}) };
  // Remove all variation dimension names from base aspects
  delete aspects['Color']; delete aspects['color'];
  delete aspects['Size'];  delete aspects['size'];
  if (product._primaryDimName)   { delete aspects[product._primaryDimName]; delete aspects[product._primaryDimName.toLowerCase()]; }
  if (product._secondaryDimName) { delete aspects[product._secondaryDimName]; delete aspects[product._secondaryDimName.toLowerCase()]; }

  // Auto-fill required item specifics
  try {
    const catMeta = await fetch(
      `${EBAY_API}/commerce/taxonomy/v1/category_tree/0/get_item_aspects_for_category?category_id=${categoryId}`,
      { headers: { Authorization: `Bearer ${access_token}`, 'Accept-Language': 'en-US' } }
    ).then(r => r.json()).catch(() => ({}));
    for (const asp of (catMeta.aspects || [])) {
      const name = asp.aspectConstraint?.aspectRequired ? asp.localizedAspectName : null;
      if (!name || aspects[name]) continue;
      const vals = (asp.aspectValues || []).map(v => v.localizedValue);
      if (!vals.length) continue;
      const match = vals.find(v => (product.title || '').toLowerCase().includes(v.toLowerCase())) || vals[0];
      aspects[name] = [match];
    }
  } catch(e) { console.warn('[push] aspects fetch failed:', e.message); }

  const locationKey = await ensureLocation(auth, false);
  const groupSku    = `DS-${Date.now()}-${Math.random().toString(36).slice(2,7).toUpperCase()}`;
  const basePrice   = parseFloat(product.price || product.cost || product.myPrice || 0);

  // ── SIMPLE ────────────────────────────────────────────────────────────────
  if (!product.hasVariations || !product.variations?.length) {
    const ebayPrice = applyMk(basePrice);
    const finalPrice = (ebayPrice > 0 ? ebayPrice : parseFloat(product.myPrice || 9.99)).toFixed(2);
    console.log(`[push/simple] cost=$${basePrice} → $${finalPrice}`);

    const simpleQty = product.inStock !== false ? defaultQty : 0;
    console.log(`[push/simple] inStock=${product.inStock} qty=${simpleQty}`);
    const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(groupSku)}`, {
      method: 'PUT', headers: auth,
      body: JSON.stringify({
        availability: { shipToLocationAvailability: { quantity: simpleQty } },
        condition: 'NEW',
        product: { title: listingTitle, description: ebayDescription, imageUrls: product.images.slice(0, 12), aspects },
      }),
    });
    if (ir.status === 401) return res.status(401).json({ error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect.', code: 'INVENTORY_401' });
    if (!ir.ok) return res.status(400).json({ error: 'Inventory PUT failed', details: await ir.text() });

    // FIX: helper to clean up on failure — delete inventory item and offer
    async function cleanupSimple(offerIdToDelete) {
      try {
        if (offerIdToDelete) {
          await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerIdToDelete}`, { method: 'DELETE', headers: auth }).catch(() => {});
        }
        await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(groupSku)}`, { method: 'DELETE', headers: auth }).catch(() => {});
        console.log(`[push/simple] cleanup done (offerId=${offerIdToDelete || 'none'})`);
      } catch(e) { console.warn('[push/simple] cleanup error:', e.message); }
    }

    const or = await fetch(`${EBAY_API}/sell/inventory/v1/offer`, {
      method: 'POST', headers: auth,
      body: JSON.stringify(buildOffer(groupSku, finalPrice, categoryId, policies, locationKey)),
    });
    const od = await or.json();
    if (!or.ok) {
      await cleanupSimple(null);
      return res.status(400).json({ error: 'Offer creation failed', details: od });
    }

    const pr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${od.offerId}/publish`, { method: 'POST', headers: auth });
    const pd = await pr.json();
    if (pd.listingId) return res.json({ success: true, sku: groupSku, offerId: od.offerId, listingId: pd.listingId });

    const errId = (pd.errors || [])[0]?.errorId;
    if (errId === 25002) {
      const existing = (pd.errors[0]?.parameters || []).find(p => p.name === 'listingId')?.value || null;
      return res.status(400).json({ error: `Duplicate — already listed as eBay item ${existing || '(check eBay)'}. Delete it first or use Revise.`, errorId: 25002, existingListingId: existing });
    }
    // FIX: publish failed — clean up inventory item + offer so it doesn't block future pushes
    await cleanupSimple(od.offerId);
    return res.status(400).json({ error: 'Publish failed', details: pd });
  }

  // ── VARIATION ─────────────────────────────────────────────────────────────
  // Use stored dim names if available, else detect
  const primaryName   = product._primaryDimName   || null;
  const secondaryName = product._secondaryDimName || null;
  const colorGroup    = primaryName
    ? product.variations.find(v => v.name === primaryName)
    : (product.variations.find(v => /color|colour/i.test(v.name)) || product.variations[0]);
  const otherGroup    = secondaryName
    ? product.variations.find(v => v.name === secondaryName)
    : product.variations.find(v => v !== colorGroup);

  const variants = buildVariants({ product, groupSku, applyMk, defaultQty, body });
  if (!variants.length) return res.status(400).json({ error: 'No variants could be built — check that the product has valid variation values.' });

  // Block push if per-variant prices couldn't be verified
  // (all prices identical = Amazon blocked per-ASIN fetches, can't price accurately)
  if (product._pricesFailed) {
    const allPrices = [...new Set(variants.map(v => v.price))];
    if (allPrices.length === 1) {
      return res.status(400).json({
        error: `Cannot push — Amazon blocked per-variant price lookups. All ${variants.length} variants would be priced identically at $${allPrices[0]}, which is incorrect. Re-import this product later or push manually with correct prices.`,
        code: 'PRICES_FAILED',
        suggestion: 'Try importing this product again in a few minutes. If the issue persists, this listing type requires manual pricing.',
      });
    }
  }

  console.log(`[push] ${variants.length} variants (${colorGroup ? colorGroup.values.length : 0} colors × ${otherGroup ? otherGroup.values.length : 1} sizes)`);

  // PUT inventory items — test first item for 401, then batch the rest
  const createdSkus = new Set();

  async function putInventoryItem(v) {
    const itemAspects = { ...aspects };
    for (const [k, val] of Object.entries(v.dims)) itemAspects[k] = [val];
    const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(v.sku)}`, {
      method: 'PUT', headers: auth,
      body: JSON.stringify({
        availability: { shipToLocationAvailability: { quantity: v.qty } },
        condition: 'NEW',
        product: {
          title:       listingTitle,
          description: ebayDescription,
          imageUrls:   v.image ? [v.image] : product.images.slice(0, 1),
          aspects:     itemAspects,
        },
      }),
    });
    return r;
  }

  // Test first
  const testR = await putInventoryItem(variants[0]);
  if (testR.status === 401) return res.status(401).json({ error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect.', code: 'INVENTORY_401' });
  if (testR.ok || testR.status === 204) createdSkus.add(variants[0].sku);
  else console.warn(`[push] first item failed: ${testR.status} ${(await testR.text()).slice(0, 100)}`);

  // Batch the rest in groups of 15
  for (let i = 1; i < variants.length; i += 15) {
    await Promise.all(variants.slice(i, i + 15).map(async v => {
      const r = await putInventoryItem(v);
      if (r.ok || r.status === 204) createdSkus.add(v.sku);
      else console.warn(`[push] inv fail ${v.sku.slice(-20)}: ${r.status}`);
    }));
    if (i + 15 < variants.length) await sleep(100);
  }

  console.log(`[push] inventory items: ${createdSkus.size}/${variants.length}`);

  // FIX: abort early if no inventory items were created — nothing to publish
  if (createdSkus.size === 0) {
    console.warn('[push] 0 inventory items created — aborting push, no cleanup needed');
    return res.status(400).json({ error: 'Failed to create any inventory items on eBay. Check your token permissions and try again.' });
  }

  // FIX: cleanup helper — deletes all created inventory items + offers on failure
  async function cleanupVariation(offerIds = []) {
    console.log(`[push] cleanup: deleting ${createdSkus.size} inventory items + ${offerIds.length} offers`);
    try {
      // Delete all offers first
      for (const oid of offerIds) {
        await fetch(`${EBAY_API}/sell/inventory/v1/offer/${oid}`, { method: 'DELETE', headers: auth }).catch(() => {});
      }
      // Delete the group
      await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`, { method: 'DELETE', headers: auth }).catch(() => {});
      // Delete all inventory items in batches
      const skuList = [...createdSkus];
      for (let i = 0; i < skuList.length; i += 25) {
        await Promise.all(skuList.slice(i, i + 25).map(sku =>
          fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, { method: 'DELETE', headers: auth }).catch(() => {})
        ));
      }
      console.log('[push] cleanup complete');
    } catch(e) { console.warn('[push] cleanup error:', e.message); }
  }

  // Group PUT
  const groupAspects = { ...aspects };
  // Remove ALL variation dimensions (not just Color/Size) so eBay doesn't reject
  for (const vg of (product.variations || [])) {
    delete groupAspects[vg.name];
    delete groupAspects[vg.name.toLowerCase()];
  }
  const variesBy = buildVariesBy(product, colorGroup, otherGroup);
  // Validate: all specs must have at least 1 value
  const validSpecs = variesBy.specifications.filter(s => s.values?.length > 0);
  if (!validSpecs.length) return res.status(400).json({ error: 'No valid variation specifications — product needs at least one variation value.' });
  variesBy.specifications = validSpecs;

  let groupOk = false;
  for (let attempt = 1; attempt <= 3 && !groupOk; attempt++) {
    const gr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`, {
      method: 'PUT', headers: auth,
      body: JSON.stringify({
        inventoryItemGroupKey: groupSku,
        title:       listingTitle,
        description: ebayDescription,
        imageUrls:   product.images.slice(0, 12),
        variantSKUs: variants.map(v => v.sku).filter(s => createdSkus.has(s)),
        aspects:     groupAspects,
        variesBy,
      }),
    });
    if (gr.ok || gr.status === 204) { groupOk = true; console.log('[push] group ok'); }
    else {
      const gt = await gr.text();
      console.warn(`[push] group attempt ${attempt}: ${gr.status} ${gt.slice(0, 300)}`);
      if (attempt < 3) await sleep(600);
      else {
        // FIX: group PUT failed after 3 attempts — clean up all created items
        await cleanupVariation([]);
        return res.status(400).json({ error: 'Group PUT failed', details: gt.slice(0, 400) });
      }
    }
  }

  // Bulk create offers
  const allOfferIds = [];
  const failedOfferVariants = [];
  for (let i = 0; i < variants.length; i += 25) {
    const batch = variants.slice(i, i + 25).map(v => buildOffer(v.sku, v.price, categoryId, policies, locationKey));
    const or = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_create_offer`, {
      method: 'POST', headers: auth, body: JSON.stringify({ requests: batch }),
    });
    const od = await or.json();
    for (const resp of (od.responses || [])) {
      if (resp.offerId) allOfferIds.push(resp.offerId);
      else {
        console.warn(`[push] offer fail ${resp.sku?.slice(-20)}: ${JSON.stringify(resp.errors?.[0]).slice(0, 150)}`);
        failedOfferVariants.push(variants.find(v => v.sku === resp.sku));
      }
    }
  }

  // Retry failed offers after 4s (location key propagation delay)
  if (failedOfferVariants.length) {
    await sleep(4000);
    for (let i = 0; i < failedOfferVariants.length; i += 25) {
      const batch = failedOfferVariants.filter(Boolean).slice(i, i + 25).map(v => buildOffer(v.sku, v.price, categoryId, policies, locationKey));
      const or = await fetch(`${EBAY_API}/sell/inventory/v1/bulk_create_offer`, {
        method: 'POST', headers: auth, body: JSON.stringify({ requests: batch }),
      });
      const od = await or.json();
      for (const resp of (od.responses || [])) {
        if (resp.offerId) allOfferIds.push(resp.offerId);
      }
    }
  }

  // Publish group
  for (let attempt = 1; attempt <= 3; attempt++) {
    const pr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/publish_by_inventory_item_group`, {
      method: 'POST', headers: auth,
      body: JSON.stringify({ inventoryItemGroupKey: groupSku, marketplaceId: 'EBAY_US' }),
    });
    const pd = await pr.json();
    if (pd.listingId) {
      console.log(`[push] published! listingId=${pd.listingId} variants=${variants.length}`);
      return res.json({ success: true, sku: groupSku, listingId: pd.listingId, variantsCreated: variants.length });
    }
    const errId = (pd.errors || [])[0]?.errorId;
    console.warn(`[push] publish attempt ${attempt}: ${JSON.stringify(pd).slice(0, 300)}`);
    if (errId === 25002) {
      const existing = (pd.errors[0]?.parameters || []).find(p => p.name === 'listingId')?.value || null;
      return res.status(400).json({ error: `Duplicate — already listed as eBay item ${existing || '(check eBay)'}. Delete it first or use Revise.`, errorId: 25002, existingListingId: existing });
    }
    // Auto-fill any missing required aspects and retry
    for (const err of (pd.errors || [])) {
      for (const param of (err.parameters || [])) {
        if (!aspects[param.value]) aspects[param.value] = ['Unbranded'];
      }
    }
    if (attempt < 3) await sleep(800);
  }
  // FIX: publish failed after 3 attempts — clean up everything so it doesn't block future pushes
  await cleanupVariation(allOfferIds);
  return res.status(400).json({ error: 'Publish failed after 3 attempts' });
}

// ─── REVISE action ─────────────────────────────────────────────────────────────

async function handleRevise({ body, res, getCategories, aiEnrich, sanitizeTitle, sleep, getEbayUrls }) {
  const sandbox  = body.sandbox === true || body.sandbox === 'true';
  const EBAY_API = getEbayUrls(sandbox).EBAY_API;
  const auth     = { Authorization: `Bearer ${body.access_token}`, 'Content-Type': 'application/json', 'Content-Language': 'en-US', 'Accept-Language': 'en-US' };
  const { access_token, ebaySku, sourceUrl } = body;
  const ebayListingId = body.ebayListingId || '';
  if (!access_token || !ebaySku || !sourceUrl)
    return res.status(400).json({ error: 'Missing access_token, ebaySku, or sourceUrl' });

  const markupPct      = parseFloat(body.markup ?? 0);
  const handling       = parseFloat(body.handlingCost ?? 2);
  const defaultQty     = parseInt(body.quantity) || 1;
  const amazonShipping = parseFloat(body.fallbackShipping || 0); // passed from cached product
  // Price formula: (amazonCost + amazonShipping + handling) × markup ÷ (1−fee) + $0.30
  const applyMk = (cost) => {
    const c = parseFloat(cost) || 0;
    if (c <= 0) return 0;
    return Math.max(
      Math.ceil(((c + amazonShipping + handling) * (1 + markupPct / 100) / (1 - 0.1335) + 0.30) * 100) / 100,
      0.99
    );
  };

  console.log(`[revise] sku=${ebaySku?.slice(0,30)} markup=${markupPct}%`);

  // ── STEP 1: Get product data ──────────────────────────────────────────────
  // Priority: client-provided HTML → fresh Amazon scrape → cached fallback → eBay existing data
  const clientHtmlRevise = body.clientHtml || null;
  let product = null;

  if (clientHtmlRevise) {
    // Browser-fetched HTML provided — use it directly, bypasses Vercel IP blocking
    console.log('[revise] using client-provided HTML for scrape');
    product = await scrapeAmazonProduct(sourceUrl, clientHtmlRevise).catch(() => null);
  } else {
    // Server-side scrape fallback
    const baseUrl = process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : 'https://dropsync-one.vercel.app';
    const scrapeR = await fetch(`${baseUrl}/api/ebay?action=scrape`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: sourceUrl }),
    }).catch(() => null);
    const scrapeD = scrapeR?.ok ? await scrapeR.json().catch(() => null) : null;
    product = scrapeD?.product || null;
  }

  // Apply cached fallback for missing images/data
  const fallbackImages  = (body.fallbackImages || []).filter(u => typeof u === 'string' && u.startsWith('http'));
  const fallbackTitle   = body.fallbackTitle || '';
  const fallbackPrice   = parseFloat(body.fallbackPrice) || 0;
  const fallbackInStock = body.fallbackInStock !== false;

  // Also accept full product data passed directly from browser (avoids re-scrape)
  const fallbackComboAsin        = body.fallbackComboAsin        || null;
  const fallbackComboInStock     = body.fallbackComboInStock     || null;
  const fallbackComboPrices      = body.fallbackComboPrices      || null;
  const fallbackVariations       = Array.isArray(body.fallbackVariations) ? body.fallbackVariations : null;
  const fallbackVariationImages  = body.fallbackVariationImages  || null;
  const fallbackPrimaryDimName   = body.fallbackPrimaryDimName   || null;
  const fallbackSecondaryDimName = body.fallbackSecondaryDimName || null;

  if (!product) {
    if (fallbackImages.length) {
      console.log(`[revise] scrape failed — using ${fallbackImages.length} cached images + combo data`);
      product = {
        title: fallbackTitle || 'Product', price: fallbackPrice,
        images: fallbackImages, inStock: fallbackInStock,
        hasVariations: !!(fallbackVariations?.length || fallbackComboAsin),
        variations:       fallbackVariations       || [],
        variationImages:  fallbackVariationImages  || {},
        comboPrices:      fallbackComboPrices      || {},
        sizePrices:       {},
        comboAsin:        fallbackComboAsin        || {},
        comboInStock:     fallbackComboInStock     || {},
        aspects: {}, breadcrumbs: [], bullets: [], descriptionPara: '',
        _primaryDimName:   fallbackPrimaryDimName,
        _secondaryDimName: fallbackSecondaryDimName,
      };
    }
  } else {
    // Merge fallback data into scraped product where needed
    if (!product.images?.length && fallbackImages.length) product.images = fallbackImages;
    // Use fallback combo data if:
    // (a) scrape returned no comboAsin, OR
    // (b) scrape returned comboAsin but comboInStock is completely empty (sub-fetches blocked)
    const scrapedComboEmpty = !Object.keys(product.comboAsin||{}).length;
    const scrapedStockEmpty = Object.keys(product.comboAsin||{}).length > 0 &&
                              Object.keys(product.comboInStock||{}).length === 0;
    if ((scrapedComboEmpty || scrapedStockEmpty) && fallbackComboAsin) {
      product.comboAsin        = fallbackComboAsin;
      product.comboInStock     = fallbackComboInStock || {};
      product.comboPrices      = fallbackComboPrices  || {};
      product.variations       = fallbackVariations   || product.variations;
      product.variationImages  = fallbackVariationImages || product.variationImages;
      if (fallbackPrimaryDimName)   product._primaryDimName   = fallbackPrimaryDimName;
      if (fallbackSecondaryDimName) product._secondaryDimName = fallbackSecondaryDimName;
      product.hasVariations = true;
      console.log('[revise] merged fallback combo data into scraped product');
    }
  }

  // Last resort: pull from eBay's existing inventory
  if (!product || !product.images?.length) {
    console.log('[revise] no product data — fetching from eBay inventory');
    try {
      // Check for variation group first
      const grpR = await fetch(
        `${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`,
        { headers: auth }
      ).then(r => r.ok ? r.json() : null).catch(() => null);
      const existingVarSkus = grpR?.variantSKUs || [];

      if (existingVarSkus.length) {
        const allImgs = [];
        const comboAsin = {}, comboInStock = {};
        for (let i = 0; i < existingVarSkus.length; i += 20) {
          const qs = existingVarSkus.slice(i, i+20).map(s => `sku=${encodeURIComponent(s)}`).join('&');
          const bd = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?${qs}`, { headers: auth }).then(r => r.json()).catch(() => ({}));
          for (const item of (bd.inventoryItems || [])) {
            const asp = item.inventoryItem?.product?.aspects || {};
            const color = (asp['Color'] || asp['color'] || [])[0] || '';
            const size  = (asp['Size']  || asp['size']  || [])[0]  || '';
            const key   = `${color}|${size}`;
            const qty   = item.inventoryItem?.availability?.shipToLocationAvailability?.quantity ?? 1;
            comboAsin[key] = item.sku;
            comboInStock[key] = qty > 0;
            (item.inventoryItem?.product?.imageUrls || []).forEach(u => { if (!allImgs.includes(u)) allImgs.push(u); });
          }
        }
        if (allImgs.length) {
          product = {
            title: fallbackTitle || 'Product', price: fallbackPrice,
            images: allImgs, inStock: Object.values(comboInStock).some(Boolean),
            hasVariations: true, variations: [], variationImages: {},
            comboPrices: {}, sizePrices: {}, comboAsin, comboInStock,
            aspects: {}, breadcrumbs: [], bullets: [], descriptionPara: '',
          };
        }
      }

      // Simple listing fallback
      if (!product || !product.images?.length) {
        const invR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, { headers: auth });
        if (invR.ok) {
          const invD = await invR.json();
          const ebayImgs = invD.product?.imageUrls || [];
          if (ebayImgs.length) {
            product = {
              title: fallbackTitle || invD.product?.title || 'Product',
              price: fallbackPrice,
              images: ebayImgs,
              inStock: (invD.availability?.shipToLocationAvailability?.quantity ?? 1) > 0,
              hasVariations: false, variations: [], variationImages: {},
              comboPrices: {}, sizePrices: {}, comboAsin: {}, comboInStock: {},
              aspects: invD.product?.aspects || {},
              breadcrumbs: [], bullets: [], descriptionPara: '',
            };
          }
        }
      }
    } catch(e) { console.warn('[revise] eBay fallback failed:', e.message); }
  }

  if (!product || !product.images?.length) {
    return res.status(503).json({
      error: 'Amazon is blocking right now and no cached data available. Will retry next cycle.',
      skippable: true,
    });
  }

  console.log(`[revise] product: "${product.title?.slice(0,50)}" imgs=${product.images.length} hasVar=${product.hasVariations} comboAsin=${Object.keys(product.comboAsin||{}).length}`);

  // ── STEP 2: AI enrichment ─────────────────────────────────────────────────
  const suggestions  = await getCategories(product.title || '', access_token).catch(() => []);
  const ai           = await aiEnrich(product.title, product.breadcrumbs || [], product.aspects || {}, suggestions).catch(() => null);
  const categoryId   = ai?.categoryId || suggestions[0]?.id || '11450';
  const listingTitle = sanitizeTitle(product.ebayTitle || ai?.title || product.title || 'Product');

  const ebayDescription = buildDescription(listingTitle, product.bullets || [], product.descriptionPara || '', product.aspects || {})
    || product.description || listingTitle;

  const aspects = { ...(product.aspects || {}), ...(ai?.aspects || {}) };
  // Remove all variation dimension names from base aspects
  delete aspects['Color']; delete aspects['color'];
  delete aspects['Size'];  delete aspects['size'];
  if (product._primaryDimName)   { delete aspects[product._primaryDimName]; delete aspects[product._primaryDimName.toLowerCase()]; }
  if (product._secondaryDimName) { delete aspects[product._secondaryDimName]; delete aspects[product._secondaryDimName.toLowerCase()]; }

  // Auto-fill required aspects
  try {
    const catMeta = await fetch(
      `${EBAY_API}/commerce/taxonomy/v1/category_tree/0/get_item_aspects_for_category?category_id=${categoryId}`,
      { headers: { Authorization: `Bearer ${access_token}`, 'Accept-Language': 'en-US' } }
    ).then(r => r.json()).catch(() => ({}));
    for (const asp of (catMeta.aspects || [])) {
      const name = asp.aspectConstraint?.aspectRequired ? asp.localizedAspectName : null;
      if (!name || aspects[name]) continue;
      const vals = (asp.aspectValues || []).map(v => v.localizedValue);
      if (!vals.length) continue;
      const match = vals.find(v => (product.title || '').toLowerCase().includes(v.toLowerCase())) || vals[0];
      aspects[name] = [match];
    }
  } catch(e) { console.warn('[revise] aspects failed:', e.message); }

  // ── STEP 3: Get existing variant SKUs from eBay ───────────────────────────
  const grpRes = await fetch(
    `${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`,
    { headers: auth }
  ).then(r => r.json()).catch(() => null);
  let variantSkus = grpRes?.variantSKUs || [];

  // If group exists but variantSKUs is empty (eBay desync bug), try to recover
  // via the offer API — offers know their SKU, and we can search by listing ID or prefix
  if (!variantSkus.length && product.hasVariations) {
    console.log('[revise] group has no variantSKUs — attempting recovery via offer API');
    try {
      // Page through all offers to find ones belonging to this listing
      let offset = 0, found = [];
      while (offset < 300) {
        const oRes = await fetch(
          `${EBAY_API}/sell/inventory/v1/offer?limit=100&offset=${offset}`,
          { headers: auth }
        ).then(r => r.json()).catch(() => ({}));
        const offers = oRes.offers || [];
        if (!offers.length) break;
        // Match by listingId or by SKU prefix
        const matched = offers.filter(o =>
          o.listing?.listingId === ebayListingId ||
          o.sku?.startsWith(ebaySku + '-') ||
          (o.sku !== ebaySku && o.sku?.startsWith(ebaySku.slice(0, 20)))
        );
        found.push(...matched.map(o => o.sku));
        if (offers.length < 100) break;
        offset += 100;
      }
      if (found.length) {
        variantSkus = [...new Set(found)].filter(s => s !== ebaySku);
        console.log(`[revise] recovered ${variantSkus.length} variant SKUs from offers`);
      }
    } catch(e) { console.warn('[revise] SKU recovery failed:', e.message); }
  }

  const isVariation = variantSkus.length > 0 || product.hasVariations;
  console.log(`[revise] mode=${variantSkus.length > 0 ? 'variation' : (product.hasVariations ? 'variation-rebuild' : 'simple')} existingVarSkus=${variantSkus.length}`);

  // ── STEP 4: Update listing ────────────────────────────────────────────────
  if (isVariation) {
    // If we have no existing SKUs, build them fresh from product variations (like push)
    // This handles the eBay group desync case + first revise after push failure
    if (!variantSkus.length && product.hasVariations && Object.keys(product.comboAsin || {}).length > 0) {
      console.log('[revise] no existing SKUs — rebuilding from product variations (re-push mode)');
      const newVariants = buildVariants({ product, groupSku: ebaySku, applyMk, defaultQty, body });
      console.log(`[revise] built ${newVariants.length} new variant SKUs`);
      // PUT each new variant
      const rebuildCreated = new Set();
      // Test first
      if (newVariants.length) {
        const v0 = newVariants[0];
        const itemAsp0 = { ...aspects };
        for (const [k, val] of Object.entries(v0.dims)) itemAsp0[k] = [val];
        const r0 = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(v0.sku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: v0.qty } },
            condition: 'NEW',
            product: { title: listingTitle, description: ebayDescription,
              imageUrls: v0.image ? [v0.image] : product.images.slice(0,1), aspects: itemAsp0 },
          }),
        });
        if (r0.status === 401) return res.status(401).json({ error: 'eBay token expired', code: 'INVENTORY_401' });
        if (r0.ok || r0.status === 204) rebuildCreated.add(v0.sku);
        else { const t = await r0.text(); console.warn(`[revise-rebuild] first item: ${r0.status} ${t.slice(0,100)}`); }
      }
      // Rest in batches
      for (let i = 1; i < newVariants.length; i += 15) {
        await Promise.all(newVariants.slice(i, i+15).map(async v => {
          const itemAsp = { ...aspects };
          for (const [k, val] of Object.entries(v.dims)) itemAsp[k] = [val];
          const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(v.sku)}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({
              availability: { shipToLocationAvailability: { quantity: v.qty } },
              condition: 'NEW',
              product: { title: listingTitle, description: ebayDescription,
                imageUrls: v.image ? [v.image] : product.images.slice(0,1), aspects: itemAsp },
            }),
          });
          if (r.ok || r.status === 204) rebuildCreated.add(v.sku);
          else { const t = await r.text(); console.warn(`[revise-rebuild] ${v.sku.slice(-20)}: ${r.status} ${t.slice(0,80)}`); }
        }));
        if (i + 15 < newVariants.length) await sleep(100);
      }
      // Group PUT with new SKUs
      const newVariesBy = buildVariesBy(product,
        product.variations?.find(v => v.name === product._primaryDimName) || product.variations?.[0],
        product.variations?.find(v => v.name === product._secondaryDimName));
      const grpAsp = { ...aspects };
      for (const vg of (product.variations||[])) { delete grpAsp[vg.name]; delete grpAsp[vg.name.toLowerCase()]; }
      for (let attempt = 1; attempt <= 3; attempt++) {
        const gr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({
            inventoryItemGroupKey: ebaySku,
            title: listingTitle, description: ebayDescription,
            imageUrls: product.images.slice(0,12),
            variantSKUs: newVariants.map(v => v.sku).filter(s => rebuildCreated.has(s)),
            aspects: grpAsp, variesBy: newVariesBy,
          }),
        });
        if (gr.ok || gr.status === 204) { console.log('[revise-rebuild] group PUT ok'); break; }
        const gt = await gr.text();
        console.warn(`[revise-rebuild] group attempt ${attempt}: ${gr.status} ${gt.slice(0,200)}`);
        if (attempt < 3) await sleep(600);
      }
      // Update offer prices
      let rebuildPrices = 0;
      for (let i = 0; i < newVariants.length; i += 8) {
        await Promise.all(newVariants.slice(i, i+8).filter(v => rebuildCreated.has(v.sku)).map(async v => {
          const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(v.sku)}`, {headers:auth}).then(r=>r.json()).catch(()=>({}));
          const offerId = (ol.offers||[])[0]?.offerId;
          if (offerId) {
            await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerId}`, {
              method:'PUT', headers:auth,
              body: JSON.stringify({pricingSummary:{price:{value:parseFloat(v.price).toFixed(2),currency:'USD'}}}),
            }).catch(()=>{});
            rebuildPrices++;
          }
        }));
        if (i + 8 < newVariants.length) await sleep(100);
      }
      console.log(`[revise-rebuild] done — ${rebuildCreated.size}/${newVariants.length} items, ${rebuildPrices} prices`);
      return res.json({
        success: true, type: 'variant-rebuild',
        updatedVariants: rebuildCreated.size, pricesUpdated: rebuildPrices,
        failed: newVariants.length - rebuildCreated.size, total: newVariants.length,
        images: product.images.length, price: applyMk(product.price || 0),
        inStock: product.inStock !== false, title: listingTitle,
        priceChanges: [], stockChanges: [], imageChanges: [],
      });
    }

    // Read existing Color/Size aspects for each SKU from eBay
    const skuAspects = {}; // sku → { Color, Size, [otherDim]: value }
    for (let i = 0; i < variantSkus.length; i += 20) {
      const qs = variantSkus.slice(i, i+20).map(s => `sku=${encodeURIComponent(s)}`).join('&');
      const bd = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?${qs}`, { headers: auth }).then(r => r.json()).catch(() => ({}));
      for (const item of (bd.inventoryItems || [])) {
        const asp = item.inventoryItem?.product?.aspects || {};
        skuAspects[item.sku] = {};
        for (const [k, v] of Object.entries(asp)) {
          skuAspects[item.sku][k] = v?.[0] || null;
        }
      }
      if (i + 20 < variantSkus.length) await sleep(100);
    }
    console.log(`[revise] read aspects for ${Object.keys(skuAspects).length}/${variantSkus.length} SKUs`);

    // Use product's actual primary/secondary dimension names (set by scraper)
    // Fallback: primary=Color (or first aspect), secondary=everything-else
    const revPrimaryName   = product._primaryDimName   || null;
    const revSecondaryName = product._secondaryDimName || null;

    // Build image map keyed by primary dimension value
    const imgByPrimary = Object.assign(
      {},
      product.variationImages?.['Color']  || {},
      revPrimaryName ? (product.variationImages?.[revPrimaryName] || {}) : {}
    );
    const imgByPrimaryLower = Object.fromEntries(Object.entries(imgByPrimary).map(([k,v]) => [k.toLowerCase(), v]));

    function getImageForPrimary(pVal) {
      if (!pVal) return product.images[0] || '';
      return imgByPrimary[pVal] || imgByPrimaryLower[pVal.toLowerCase()] || product.images[0] || '';
    }

    // Extract primary/secondary values from a SKU's aspects dict
    // Works for Color+Size, Style-only, Size-only, Color-only, etc.
    function getPrimarySecondary(dimVals) {
      if (!dimVals || !Object.keys(dimVals).length) return { pVal: null, sVal: null };
      if (revPrimaryName && revSecondaryName) {
        // We know exact names from scraper
        return { pVal: dimVals[revPrimaryName] || null, sVal: dimVals[revSecondaryName] || null };
      }
      if (revPrimaryName) {
        // Only primary known — secondary is whatever else is there
        const pVal = dimVals[revPrimaryName] || null;
        const sVal = Object.entries(dimVals).find(([k,v]) => k !== revPrimaryName && v)?.[1] || null;
        return { pVal, sVal };
      }
      // Fallback: primary = Color/Colour if present, else first aspect; secondary = rest
      const colorEntry = Object.entries(dimVals).find(([k]) => /color|colour/i.test(k));
      if (colorEntry) {
        const pVal = colorEntry[1];
        const sVal = Object.entries(dimVals).find(([k,v]) => !/color|colour/i.test(k) && v)?.[1] || null;
        return { pVal, sVal };
      }
      // No color — first aspect is primary
      const entries = Object.entries(dimVals).filter(([,v]) => v);
      return { pVal: entries[0]?.[1] || null, sVal: entries[1]?.[1] || null };
    }

    // Stock & price per SKU using comboAsin/comboInStock from scraped data
    const comboAsin    = product.comboAsin    || {};
    const comboInStock = product.comboInStock || {};
    const comboPrices  = product.comboPrices  || {};
    const sizePrices   = product.sizePrices   || {};
    const hasComboData = Object.keys(comboAsin).length > 0;
    const freshStock   = product.inStock !== false;
    const basePrice    = parseFloat(product.price || 0);

    function getQty(pVal, sVal) {
      // No combo data at all → assume in stock (better to show in-stock than wrongly OOS)
      if (!hasComboData) return defaultQty;
      const key = `${pVal || ''}|${sVal || ''}`;
      // Combo doesn't exist on Amazon at all → OOS
      if (!comboAsin[key]) return 0;
      // Explicitly marked OOS in per-combo data → OOS
      if (comboInStock[key] === false) return 0;
      // comboInStock[key] = true OR undefined (unknown) → in stock
      // NOTE: do NOT use product-level freshStock here — it can be stale Railway data
      return defaultQty;
    }

    function getPrice(pVal, sVal) {
      const key = `${pVal || ''}|${sVal || ''}`;
      const amazonPrice = comboPrices[key] || sizePrices[sVal || ''] || sizePrices[pVal || ''] || basePrice || 0;
      const p = applyMk(amazonPrice);
      return p > 0 ? p : applyMk(basePrice) || 9.99;
    }

    // PUT all variant inventory items
    const createdSkus = new Set();
    const failedSkus  = [];

    // Test first item
    if (variantSkus.length > 0) {
      const sku0 = variantSkus[0];
      const dimVals = skuAspects[sku0] || {};
      const { pVal: pVal0, sVal: sVal0 } = getPrimarySecondary(dimVals);
      const itemAsp  = { ...aspects, ...Object.fromEntries(Object.entries(dimVals).filter(([,v]) => v).map(([k,v]) => [k, [v]])) };
      const r0 = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku0)}`, {
        method: 'PUT', headers: auth,
        body: JSON.stringify({
          availability: { shipToLocationAvailability: { quantity: getQty(pVal0, sVal0) } },
          condition: 'NEW',
          product: {
            title: listingTitle, description: ebayDescription,
            imageUrls: getImageForPrimary(pVal0) ? [getImageForPrimary(pVal0)] : product.images.slice(0, 1),
            aspects: itemAsp,
          },
        }),
      });
      if (r0.status === 401) return res.status(401).json({ error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect.', code: 'INVENTORY_401' });
      if (r0.ok || r0.status === 204) createdSkus.add(sku0);
      else { const t = await r0.text(); console.warn(`[revise] first item fail: ${r0.status} ${t.slice(0,100)}`); failedSkus.push(sku0); }
    }

    // Rest in batches of 15
    for (let i = 1; i < variantSkus.length; i += 15) {
      await Promise.all(variantSkus.slice(i, i + 15).map(async sku => {
        const dimVals  = skuAspects[sku] || {};
        const { pVal, sVal } = getPrimarySecondary(dimVals);
        const itemAsp  = { ...aspects, ...Object.fromEntries(Object.entries(dimVals).filter(([,v]) => v).map(([k,v]) => [k, [v]])) };
        const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: getQty(pVal, sVal) } },
            condition: 'NEW',
            product: {
              title: listingTitle, description: ebayDescription,
              imageUrls: getImageForPrimary(pVal) ? [getImageForPrimary(pVal)] : product.images.slice(0, 1),
              aspects: itemAsp,
            },
          }),
        });
        if (r.ok || r.status === 204) createdSkus.add(sku);
        else { const t = await r.text(); console.warn(`[revise] PUT fail ${sku.slice(-20)}: ${r.status} ${t.slice(0,80)}`); failedSkus.push(sku); }
      }));
      if (i + 15 < variantSkus.length) await sleep(100);
    }

    // Retry failed
    if (failedSkus.length) {
      await sleep(800);
      await Promise.all(failedSkus.map(async sku => {
        const dimVals  = skuAspects[sku] || {};
        const { pVal, sVal } = getPrimarySecondary(dimVals);
        const itemAsp  = { ...aspects, ...Object.fromEntries(Object.entries(dimVals).filter(([,v]) => v).map(([k,v]) => [k, [v]])) };
        const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: getQty(pVal, sVal) } },
            condition: 'NEW',
            product: {
              title: listingTitle, description: ebayDescription,
              imageUrls: getImageForPrimary(pVal) ? [getImageForPrimary(pVal)] : product.images.slice(0, 1),
              aspects: itemAsp,
            },
          }),
        });
        if (r.ok || r.status === 204) createdSkus.add(sku);
      }));
    }

    console.log(`[revise] inventory: ${createdSkus.size}/${variantSkus.length} updated, ${failedSkus.length} failed`);

    // Group PUT — rebuild variesBy using actual dimension names
    // Use scraped product data first (most accurate), fall back to reading eBay SKU aspects
    const revPrimGrp = revPrimaryName
      ? product.variations?.find(v => v.name === revPrimaryName)
      : product.variations?.find(v => /color|colour/i.test(v.name)) || product.variations?.[0];
    const revSecGrp  = revSecondaryName
      ? product.variations?.find(v => v.name === revSecondaryName)
      : product.variations?.find(v => v !== revPrimGrp);

    // Fall back to reading existing eBay aspects if no scraped data
    const allPrimVals = revPrimGrp
      ? revPrimGrp.values.map(v => v.value)
      : [...new Set(Object.values(skuAspects).map(d => d[revPrimaryName] || Object.values(d)[0]).filter(Boolean))];
    const allSecVals  = revSecGrp
      ? revSecGrp.values.map(v => v.value)
      : [...new Set(Object.values(skuAspects).flatMap(d => Object.entries(d).filter(([k]) => k !== revPrimaryName).map(([,v]) => v)).filter(Boolean))];

    // Dimension names for specs
    const primDimName = revPrimGrp?.name || revPrimaryName || Object.keys(Object.values(skuAspects)[0] || {})[0] || 'Style';
    const secDimName  = revSecGrp?.name  || revSecondaryName || Object.keys(Object.values(skuAspects)[0] || {}).find(k => k !== primDimName) || 'Size';

    const groupAspects = { ...aspects };
    for (const vg of (product.variations || [])) { delete groupAspects[vg.name]; delete groupAspects[vg.name.toLowerCase()]; }
    delete groupAspects['Color']; delete groupAspects['Size']; // always remove these too

    const specs = [];
    if (allPrimVals.length) specs.push({ name: primDimName, values: allPrimVals });
    if (allSecVals.length)  specs.push({ name: secDimName,  values: allSecVals  });

    // Image varies by primary only if it's color-like
    const isColorPrim = /color|colour/i.test(primDimName);

    for (let attempt = 1; attempt <= 3; attempt++) {
      const gr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, {
        method: 'PUT', headers: auth,
        body: JSON.stringify({
          inventoryItemGroupKey: ebaySku,
          title:       listingTitle,
          description: ebayDescription,
          imageUrls:   product.images.slice(0, 12),
          variantSKUs: variantSkus.filter(s => createdSkus.has(s)),
          aspects:     groupAspects,
          variesBy: {
            aspectsImageVariesBy: isColorPrim ? [primDimName] : [],
            specifications: specs,
          },
        }),
      });
      if (gr.ok || gr.status === 204) { console.log('[revise] group PUT ok'); break; }
      const gt = await gr.text();
      console.warn(`[revise] group attempt ${attempt}: ${gr.status} ${gt.slice(0, 200)}`);
      if (attempt < 3) await sleep(600);
    }

    // Update offer prices
    let pricesUpdated = 0;
    for (let i = 0; i < variantSkus.length; i += 8) {
      await Promise.all(variantSkus.slice(i, i + 8).map(async sku => {
        if (!createdSkus.has(sku)) return;
        const dimVals  = skuAspects[sku] || {};
        const { pVal: ppVal, sVal: psVal } = getPrimarySecondary(dimVals);
        const price    = getPrice(ppVal, psVal);
        const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth }).then(r => r.json()).catch(() => ({}));
        const offerId = (ol.offers || [])[0]?.offerId;
        if (offerId) {
          await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerId}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({ pricingSummary: { price: { value: price.toFixed(2), currency: 'USD' } } }),
          }).catch(() => {});
          pricesUpdated++;
        }
      }));
      if (i + 8 < variantSkus.length) await sleep(100);
    }

    console.log(`[revise] done — ${createdSkus.size} updated, ${pricesUpdated} prices, ${failedSkus.length} failed`);
    return res.json({
      success: true, type: 'variant',
      updatedVariants: createdSkus.size, pricesUpdated,
      failed: failedSkus.length, total: variantSkus.length,
      images: product.images.length, price: applyMk(basePrice),
      inStock: freshStock, title: listingTitle,
      priceChanges: [], stockChanges: [], imageChanges: [],
    });

  } else {
    // ── SIMPLE REVISE ─────────────────────────────────────────────────────
    // Safety: if product has variations, do NOT treat as simple — that would set group item qty=0
    if (product.hasVariations) {
      console.warn('[revise] product hasVariations but no variant SKUs found — skipping to avoid OOS wipe');
      return res.json({
        success: false,
        error: 'Cannot revise — listing has variations but eBay variant SKUs not found. Use Re-push to rebuild the listing.',
        code: 'VARIATION_SKUS_MISSING',
        suggestion: 'Click Re-push in the Listed tab to end this listing and push a fresh one.',
      });
    }
    const freshStock = product.inStock !== false;
    const newPrice   = applyMk(parseFloat(product.price || 0));
    const finalPrice = newPrice > 0 ? newPrice : (applyMk(fallbackPrice) || 9.99);
    const newQty     = freshStock ? defaultQty : 0;

    const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, {
      method: 'PUT', headers: auth,
      body: JSON.stringify({
        availability: { shipToLocationAvailability: { quantity: newQty } },
        condition: 'NEW',
        product: { title: listingTitle, description: ebayDescription, imageUrls: product.images.slice(0, 12), aspects },
      }),
    });
    if (ir.status === 401) return res.status(401).json({ error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect.', code: 'INVENTORY_401' });
    if (!ir.ok) {
      const t = await ir.text();
      console.warn('[revise/simple] inv PUT failed:', ir.status, t.slice(0, 150));
      return res.status(400).json({ error: `Inventory update failed: ${t.slice(0, 200)}` });
    }

    const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(ebaySku)}`, { headers: auth }).then(r => r.json()).catch(() => ({}));
    const offerId = (ol.offers || [])[0]?.offerId;
    if (offerId) {
      await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerId}`, {
        method: 'PUT', headers: auth,
        body: JSON.stringify({ pricingSummary: { price: { value: finalPrice.toFixed(2), currency: 'USD' } } }),
      }).catch(() => {});
    }

    console.log(`[revise/simple] price=$${finalPrice} qty=${newQty} imgs=${product.images.length}`);
    return res.json({
      success: true, type: 'simple',
      updatedVariants: 1, images: product.images.length,
      price: finalPrice, inStock: freshStock, title: listingTitle,
      priceChanges: [], stockChanges: newQty === 0 ? ['Out of stock'] : [], imageChanges: [],
    });
  }
}




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
      const sandbox = req.query.sandbox === 'true';
      const E = getEbayUrls(sandbox);
      const REDIRECT = E.REDIRECT || `${req.headers['x-forwarded-proto']||'https'}://${req.headers.host}/api/ebay?action=callback`;
      console.log(`[auth] sandbox=${sandbox} client_id=${E.CLIENT_ID?.slice(0,20)} redirect=${REDIRECT}`);
      const url = `${E.EBAY_AUTH}?client_id=${E.CLIENT_ID}&redirect_uri=${encodeURIComponent(REDIRECT)}&response_type=code&scope=${encodeURIComponent(SCOPES)}&state=${sandbox?'sandbox':'production'}`;
      return res.json({ url, sandbox });
    }

    if (action === 'callback') {
      // Detect sandbox from state param passed through OAuth flow
      const sandbox = req.query.state === 'sandbox' || req.query.sandbox === 'true' || process.env.EBAY_SANDBOX === 'true';
      const E = getEbayUrls(sandbox);
      const REDIRECT = E.REDIRECT || `${req.headers['x-forwarded-proto']||'https'}://${req.headers.host}/api/ebay?action=callback`;
      const creds = Buffer.from(`${E.CLIENT_ID}:${E.CLIENT_SECRET}`).toString('base64');
      console.error(`[callback] sandbox=${sandbox} tok_url=${E.EBAY_TOK} client=${E.CLIENT_ID?.slice(0,25)} redirect=${REDIRECT}`);
      const r = await fetch(E.EBAY_TOK, {
        method: 'POST',
        headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `grant_type=authorization_code&code=${encodeURIComponent(req.query.code)}&redirect_uri=${encodeURIComponent(REDIRECT)}`,
      });
      const d = await r.json();
      if (!d.access_token) {
        console.error('[callback] eBay token exchange failed:', JSON.stringify(d).slice(0,300));
        return res.setHeader('Content-Type','text/html').send(
          `<!DOCTYPE html><html><body><p style="font-family:sans-serif;padding:40px;color:red">
            ❌ eBay auth failed: ${d.error || d.error_description || JSON.stringify(d).slice(0,200)}
          </p></body></html>`
        );
      }
      const payload = {
        type: 'ebay_auth',
        token: d.access_token,
        refresh: d.refresh_token,
        expiry: Date.now() + ((d.expires_in||7200)-120)*1000
      };
      return res.setHeader('Content-Type','text/html').send(
        `<!DOCTYPE html><html><body>
        <p style="font-family:sans-serif;padding:40px">✅ Connected to eBay! Closing…</p>
        <script>
          var payload = ${JSON.stringify(payload)};
          // Try postMessage to opener first
          if (window.opener) {
            window.opener.postMessage(payload, '*');
            setTimeout(function(){ window.close(); }, 800);
          } else {
            // Fallback: store in localStorage then redirect back to app
            try { localStorage.setItem('ebay_auth_pending', JSON.stringify(payload)); } catch(e){}
            setTimeout(function(){ window.location.href = '/'; }, 800);
          }
        </script>
        </body></html>`
      );
    }

    if (action === 'refresh') {
      const sandbox = (body.sandbox || req.query.sandbox) === 'true';
      const E = getEbayUrls(sandbox);
      const creds = Buffer.from(`${E.CLIENT_ID}:${E.CLIENT_SECRET}`).toString('base64');
      const r = await fetch(E.EBAY_TOK, {
        method: 'POST',
        headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `grant_type=refresh_token&refresh_token=${encodeURIComponent(body.refresh_token)}&scope=${encodeURIComponent(SCOPES)}`,
      });
      const d = await r.json();
      if (!d.access_token) return res.status(400).json({ error: 'Token refresh failed', raw: d });
      return res.json({ access_token: d.access_token, expires_in: d.expires_in, expiry: Date.now() + ((d.expires_in||7200)-120)*1000 });
    }

    // ── TEST CREDS ───────────────────────────────────────────────────────────
    if (action === 'test_creds') {
      const sandbox = req.query.sandbox === 'true';
      const E = getEbayUrls(sandbox);
      // Try client credentials grant to verify client_id/secret work
      const creds = Buffer.from(`${E.CLIENT_ID}:${E.CLIENT_SECRET}`).toString('base64');
      const r = await fetch(E.EBAY_TOK, {
        method: 'POST',
        headers: { Authorization: `Basic ${creds}`, 'Content-Type': 'application/x-www-form-urlencoded' },
        body: `grant_type=client_credentials&scope=${encodeURIComponent('https://api.ebay.com/oauth/api_scope')}`,
      });
      const d = await r.json();
      return res.json({ sandbox, client_id: E.CLIENT_ID, has_secret: !!E.CLIENT_SECRET, result: d });
    }

    // ── POLICIES ─────────────────────────────────────────────────────────────
    if (action === 'policies') {
      const token = body.access_token || req.query.access_token;
      if (!token) return res.status(400).json({ error: 'No token' });
      const sandbox = body.sandbox === true || body.sandbox === 'true' || req.query.sandbox === 'true';
      const EBAY_API = getEbayUrls(sandbox).EBAY_API;
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

      // Use browser-fetched HTML if provided (bypasses Vercel IP blocking by Amazon)
      const clientHtml = body.pageHtml || null;

      // Normalize to clean dp/ASIN URL
      const asinM = url.match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})/);
      const asinP = url.match(/[?&]asin=([A-Z0-9]{10})/i);
      const asin  = asinM?.[1] || asinP?.[1];
      // Use ?th=1 to force variant page and avoid redirects
      if (asin) url = `https://www.amazon.com/dp/${asin}?th=1`;

      let html = clientHtml || null;
      if (!html) {
        const ua = randUA();
        // If first attempt blocked, try alternate URL formats
        html = await fetchPage(url, ua);
        if (!html && asin) {
          await sleep(1500);
          html = await fetchPage(`https://www.amazon.com/dp/${asin}?psc=1`, ua);
        }
        if (!html && asin) {
          await sleep(2000);
          // Try with a product slug placeholder
          html = await fetchPage(`https://www.amazon.com/product/dp/${asin}`, ua);
        }
      } else {
        console.log('[scrape] using client-provided HTML — skipping server-side Amazon fetch');
      }
      if (!html) return res.json({ success: false, error: 'Amazon is blocking requests right now. Wait 1-2 minutes and try again.' });

      const product = {
        url, source: 'amazon', asin: asin || '',
        title: '', price: 0, images: [],
        description: '', aspects: {}, breadcrumbs: [],
        variations: [], variationImages: {}, hasVariations: false,
        inStock: true, quantity: 1,
      };

      // Title
      const tM = html.match(/id="productTitle"[^>]*>\s*([^<]{5,})/);
      if (tM) product.title = tM[1].trim().replace(/\s+/g,' ');

      // Breadcrumbs (Amazon category path)
      const bcRaw = [...html.matchAll(/class="a-link-normal"[^>]*>\s*([^<]{2,40})\s*<\/a>/g)];
      product.breadcrumbs = bcRaw.slice(0, 6).map(m => m[1].trim()).filter(s => s.length > 1 && !/^\d+$/.test(s));

      // Price + shipping
      product.price        = extractPriceFromBuyBox(html) || extractPrice(html) || 0;
      product.shippingCost = extractShippingCost(html);
      // Stock — buy-box only to avoid false OOS from ads/sponsored products
      const _bb2 = (html.match(/id="availability"[\s\S]{0,3000}/)?.[0]||'')
                 + (html.match(/id="addToCart_feature_div"[\s\S]{0,1000}/)?.[0]||'');
      if (_bb2.length > 30) {
        product.inStock = !_bb2.toLowerCase().includes('currently unavailable');
      } else {
        product.inStock = html.includes('id="add-to-cart-button"')
                       || (!html.includes('id="outOfStock"') && !html.includes('Currently unavailable'));
      }

      // Images — all hiRes from page
      const imgs = [...html.matchAll(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/g)]
        .map(m => m[1]).filter((v,i,a) => a.indexOf(v)===i);
      product.images = imgs.slice(0, 12);

      // ── Description: extract feature bullets from multiple Amazon patterns ──────
      const extractBullets = (h) => {
        const results = [];

        // Pattern 1: productFactsDesktopExpander or featurebullets_feature_div
        // Find the "About this item" section content
        const sectionPatterns = [
          /id="productFactsDesktopExpander"[^>]*>([\s\S]{100,8000}?)<\/div>\s*<\/div>\s*<\/div>/,
          /id="featurebullets_feature_div"[^>]*>([\s\S]{100,6000}?)<\/div>\s*<\/div>/,
          /id="feature-bullets"[^>]*>([\s\S]{100,6000}?)<\/div>\s*<\/div>/,
          /id="FeatureBullets"[^>]*>([\s\S]{100,6000}?)<\/div>\s*<\/div>/,
        ];
        let sectionHtml = '';
        for (const pat of sectionPatterns) {
          const m = h.match(pat);
          if (m) { sectionHtml = m[1]; break; }
        }

        // Extract <li> content from the section
        if (sectionHtml) {
          const liMatches = [...sectionHtml.matchAll(/<li[^>]*>[\s\S]*?<span[^>]*class="[^"]*a-list-item[^"]*"[^>]*>([\s\S]{15,600}?)<\/span>/g)];
          for (const m of liMatches) {
            const text = m[1].replace(/<[^>]+>/g, '').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').replace(/&#\d+;/g,'').trim();
            if (text.length > 15 && !text.includes('return') && !text.includes('Your Orders')) {
              results.push(text);
            }
          }
        }

        // Pattern 2: broader a-list-item spans (fallback)
        if (!results.length) {
          const allLi = [...h.matchAll(/<li[^>]*>\s*<span[^>]*a-list-item[^"]*"[^>]*>([\s\S]{20,600}?)<\/span>/g)];
          for (const m of allLi) {
            const text = m[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').trim();
            if (text.length > 20 && !text.includes('return') && !text.includes('Your Orders') && !text.includes('Drop off')) {
              results.push(text);
            }
            if (results.length >= 8) break;
          }
        }

        return results.slice(0, 8);
      };

      const bullets = extractBullets(html);

      // Also extract product description paragraph if exists
      const descParaM = html.match(/id="productDescription"[^>]*>[\s\S]{0,200}<p[^>]*>([\s\S]{30,1000}?)<\/p>/);
      const descPara = descParaM ? descParaM[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').trim() : '';

      // Store raw bullets for building eBay HTML description later
      product.bullets = bullets;
      product.descriptionPara = descPara;

      // Build the eBay HTML description
      const buildEbayDescription = (title, buls, para, aspects) => {
        const bulletHtml = buls.length
          ? '<ul>' + buls.map(b => `<li>${b.replace(/</g,'&lt;').replace(/>/g,'&gt;')}</li>`).join('') + '</ul>'
          : '';
        const specRows = Object.entries(aspects || {})
          .filter(([k,v]) => !['ASIN','UPC','Color','Size','Brand Name','Brand'].includes(k) && v[0] && v[0].length < 80)
          .slice(0, 10)
          .map(([k,v]) => `<tr><td><b>${k}</b></td><td>${v[0]}</td></tr>`)
          .join('');
        const specsTable = specRows
          ? `<br/><table border="0" cellpadding="4" cellspacing="0" width="100%"><tbody>${specRows}</tbody></table>`
          : '';
        return [
          `<h2>${title}</h2>`,
          bulletHtml,
          para ? `<p>${para}</p>` : '',
          specsTable,
          '<br/><p style="font-size:11px;color:#888">Ships from US. Item is new. Please message us with any questions before purchasing.</p>'
        ].filter(Boolean).join('\n');
      };

      product.description = buildEbayDescription(product.title, bullets, descPara, product.aspects);
      if (!bullets.length) product.description = product.title; // fallback

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
      // CONFIRMED from live Amazon page analysis:
      //  - dimensionToAsinMap: "ci_si" → ASIN (only combos that exist & are buyable)
      //  - Each ASIN is a unique combo — prices vary per size AND per color
      //  - STRATEGY: find a "full color" (has all sizes), fetch 1 page per size (6 fetches)
      //    → gives us all 6 size prices. Apply size price to each color+size combo.
      //  - Stock: if combo key exists in dimensionToAsinMap → IN STOCK. Period.

      // 1. Swatch images
      // ── Variation parsing — handles ALL Amazon listing types ─────────────────────
      const swatchImgMap = extractSwatchImages(html);

      // Detect ALL variation dimensions from variationValues (not just color+size)
      let varVals = null;
      const vvM = html.match(/"variationValues"\s*:\s*(\{(?:[^{}]|\{[^{}]*\})*\})/);
      if (vvM) try { varVals = JSON.parse(vvM[1]); } catch {}

      // Build dimension list: any *_name key with values
      const allDims = [];
      if (varVals) {
        for (const [key, vals] of Object.entries(varVals)) {
          if (Array.isArray(vals) && vals.length) allDims.push({ key, name: dimKeyToAspectName(key), values: vals });
        }
      }
      const hasVar = allDims.length > 0;
      product.hasVariations = hasVar;

      if (hasVar) {
        // Primary dimension = Color/Colour if present, else first dim
        const primaryDim   = allDims.find(d => /color|colour/i.test(d.name)) || allDims[0];
        const secondaryDim = allDims.find(d => d !== primaryDim) || null;
        const primaryVals   = primaryDim.values;
        const secondaryVals = secondaryDim?.values || [];

        // Parse dimension order from page
        let dimOrder = null;
        const dimM = html.match(/"dimensions"\s*:\s*(\[[^\]]{0,400}\])/s);
        if (dimM) try { dimOrder = JSON.parse(dimM[1]); } catch {}
        if (!dimOrder || !dimOrder.length) dimOrder = allDims.map(d => d.key);

        // Build comboAsin map from dimensionToAsinMap
        const dtaBlock = extractBlock(html, '"dimensionToAsinMap"');
        let dtaMap = {};
        try { dtaMap = JSON.parse(dtaBlock); } catch {}

        const pIdx = dimOrder.indexOf(primaryDim.key)   >= 0 ? dimOrder.indexOf(primaryDim.key)   : 0;
        const sIdx = secondaryDim && dimOrder.indexOf(secondaryDim.key) >= 0
                   ? dimOrder.indexOf(secondaryDim.key)
                   : (pIdx === 0 ? 1 : 0);

        const comboAsin = {};      // "PrimaryVal|SecondaryVal" → ASIN
        const primaryToAsins = {}; // primaryVal → [ASINs]
        for (const [code, asin] of Object.entries(dtaMap)) {
          const parts = code.split('_').map(Number);
          const pVal  = primaryVals[parts[pIdx]];
          const sVal  = secondaryDim ? (secondaryVals[parts[sIdx]] ?? '') : '';
          if (!pVal) continue;
          const key = `${pVal}|${sVal}`;
          comboAsin[key] = asin;
          if (!primaryToAsins[pVal]) primaryToAsins[pVal] = [];
          if (!primaryToAsins[pVal].includes(asin)) primaryToAsins[pVal].push(asin);
        }

        // Fallback: extract from color→ASIN patterns in HTML (handles some edge cases)
        if (!Object.keys(comboAsin).length) {
          const { colorToAsin: ctaMap } = extractColorAsinMaps(html);
          for (const [c, asin] of Object.entries(ctaMap)) {
            comboAsin[`${c}|`] = asin;
            primaryToAsins[c] = [asin];
          }
        }

        // ── Stock + Price per combo ───────────────────────────────────────────────
        // Strategy:
        //   1. Get baseInStock from main page buy-box (reliable, current selection)
        //   2. Fetch per-ASIN pages for price + stock
        //   3. For stock: ONLY mark OOS if buy-box EXPLICITLY says "currently unavailable"
        //      If blocked/ambiguous → fall back to baseInStock (main page result)
        //   4. This avoids false OOS from Vercel's server location + ads on page

        // baseInStock: what the main page currently shows for the selected variant
        const bbMain = (html.match(/id="availability"[\s\S]{0,3000}/)?.[0] || '')
                     + (html.match(/id="addToCart_feature_div"[\s\S]{0,1000}/)?.[0] || '');
        const mainInStock = bbMain.length > 30
          ? !bbMain.toLowerCase().includes('currently unavailable')
          : !html.includes('id="outOfStock"') && !html.includes('Currently unavailable');

        // Pick a "fullDim" primary value to fetch per-secondary prices from
        // Prefer the one with most secondary values mapped (likely full coverage)
        const fullPrimary = Object.entries(primaryToAsins)
          .sort((a,b) => b[1].length - a[1].length)[0]?.[0] || primaryVals[0];

        // Build secondary→ASIN map for the fullPrimary
        const secToFetchAsin = {};
        for (const [key, asin] of Object.entries(comboAsin)) {
          const [pv, sv] = key.split('|');
          if (pv === fullPrimary) secToFetchAsin[sv] = asin;
        }
        if (!Object.keys(secToFetchAsin).length && Object.keys(comboAsin).length) {
          // Color-only or can't find fullPrimary combos — use any asin
          const firstAsin = Object.values(comboAsin)[0];
          secToFetchAsin[''] = firstAsin;
        }

        const asinInStock = {};  // ASIN → true/false/undefined
        const asinPrice   = {};  // ASIN → price

        // Fetch per-secondary-value pages for price + stock
        await Promise.all(Object.entries(secToFetchAsin).map(async ([sVal, asin]) => {
          if (!asin) return;
          const h = await fetchPage(`https://www.amazon.com/dp/${asin}`, ua);
          if (!h) return; // blocked → leave undefined (will use mainInStock)
          const price = extractPriceFromBuyBox(h);
          if (price) asinPrice[asin] = price;
          const bb = (h.match(/id="availability"[\s\S]{0,3000}/)?.[0] || '')
                   + (h.match(/id="addToCart_feature_div"[\s\S]{0,1000}/)?.[0] || '');
          if (bb.length > 30) {
            asinInStock[asin] = !bb.toLowerCase().includes('currently unavailable');
          }
          // If bb is short/empty: leave undefined → will use mainInStock fallback
        }));

        // Build per-secondary stock + price maps
        const secInStock = {};  // secondaryVal → true/false
        const sizePrices = {};  // secondaryVal → price
        for (const [sVal, asin] of Object.entries(secToFetchAsin)) {
          if (asinInStock[asin] !== undefined) secInStock[sVal] = asinInStock[asin];
          if (asinPrice[asin]) sizePrices[sVal] = asinPrice[asin];
        }

        // Fill missing prices with main page price
        const mainPrice = extractPriceFromBuyBox(html) || product.price || 0;
        for (const sv of secondaryVals) { if (!sizePrices[sv]) sizePrices[sv] = mainPrice; }
        if (!secondaryVals.length && !sizePrices['']) sizePrices[''] = mainPrice;

        // Build comboInStock + comboPrices
        const comboInStock = {};
        const comboPrices  = {};
        for (const [key, asin] of Object.entries(comboAsin)) {
          const [, sv] = key.split('|');
          // Price: per-ASIN → per-secondary → main page
          comboPrices[key] = asinPrice[asin] || sizePrices[sv] || mainPrice;
          // Stock: explicit per-ASIN → per-secondary level → mainInStock fallback
          if (asinInStock[asin] !== undefined) {
            comboInStock[key] = asinInStock[asin];
          } else if (secInStock[sv] !== undefined) {
            comboInStock[key] = secInStock[sv];
          } else {
            // Unknown — default to whatever the main page shows
            comboInStock[key] = mainInStock;
          }
        }

        // product.inStock = true if ANY combo is in stock
        product.inStock = Object.keys(comboInStock).length > 0
          ? Object.values(comboInStock).some(v => v !== false)
          : mainInStock;

        // Best price = cheapest in-stock secondary value
        const inStockPrices = Object.entries(comboPrices)
          .filter(([k]) => comboInStock[k] !== false)
          .map(([,p]) => p)
          .filter(p => p > 0);
        if (inStockPrices.length) product.price = Math.min(...inStockPrices);
        else { const allPrices = Object.values(comboPrices).filter(p=>p>0); if (allPrices.length) product.price = Math.min(...allPrices); }

        // ── Per-color images ──────────────────────────────────────────────────────
        const colorImgMap = {};
        for (const pv of primaryVals) colorImgMap[pv] = swatchImgMap[pv] || '';
        // Fetch images for primaries that don't have swatch images
        const needImg = primaryVals.filter(pv => !colorImgMap[pv]);
        if (needImg.length) {
          await Promise.all(needImg.slice(0, 8).map(async pv => {
            const asin = primaryToAsins[pv]?.[0];
            if (!asin) return;
            const h = await fetchPage(`https://www.amazon.com/dp/${asin}`, ua);
            if (h) { const img = extractMainImage(h); if (img) colorImgMap[pv] = img; }
          }));
        }
        // Add color images to product.images
        for (const pv of primaryVals) {
          const img = colorImgMap[pv];
          if (img && !product.images.includes(img)) product.images.push(img);
        }
        // Fallback: assign from product.images for any still-missing
        let fi = 0;
        for (const pv of primaryVals) {
          if (!colorImgMap[pv]) colorImgMap[pv] = product.images[fi++ % Math.max(1, product.images.length)] || '';
        }

        // ── Build variation groups ────────────────────────────────────────────────
        // Primary dimension group (e.g. Color)
        product.variations.push({
          name: primaryDim.name,
          values: primaryVals.map(pv => {
            const inStock = Object.entries(comboInStock)
              .some(([k,v]) => k.startsWith(`${pv}|`) && v !== false);
            const pvPrices = Object.entries(comboPrices)
              .filter(([k]) => k.startsWith(`${pv}|`)).map(([,p]) => p).filter(p=>p>0);
            return {
              value:   pv,
              price:   pvPrices.length ? Math.min(...pvPrices) : mainPrice,
              image:   colorImgMap[pv] || '',
              inStock: inStock,
              enabled: inStock,
            };
          }),
        });
        // Secondary dimension group (e.g. Size, Style)
        if (secondaryDim) {
          product.variations.push({
            name: secondaryDim.name,
            values: secondaryVals.map(sv => {
              const inStock = Object.entries(comboInStock)
                .some(([k,v]) => k.endsWith(`|${sv}`) && v !== false);
              return {
                value:   sv,
                price:   sizePrices[sv] || mainPrice,
                inStock: inStock,
                enabled: inStock,
                image:   '',
              };
            }),
          });
        }

        // Store on product
        product.comboAsin     = comboAsin;
        product.comboInStock  = comboInStock;
        product.sizePrices    = sizePrices;
        product.comboPrices   = comboPrices;
        product.variationImages[primaryDim.name] = Object.fromEntries(
          primaryVals.map(pv => [pv, colorImgMap[pv]]).filter(([,img]) => img)
        );
        // Store primary/secondary dim names for push/revise to use
        product._primaryDimName   = primaryDim.name;
        product._secondaryDimName = secondaryDim?.name || null;
      }

      const colorGrp = product.variations.find(v=>v.name==='Color');
      const pricesFound = colorGrp ? colorGrp.values.filter(v=>v.price>0).length : 0;
      const imagesFound = colorGrp ? colorGrp.values.filter(v=>v.image).length : 0;
      console.log(`[scrape] OK "${product.title.slice(0,50)}" price=$${product.price} colors=${colorGrp?.values.length||0} prices=${pricesFound} images=${imagesFound} imgs=${product.images.length}`);

      // Build colorAsinMap for worker: color name → ASIN
      // This lets the Railway worker fetch per-color prices independently
      const colorAsinMap = {};
      if (colorGrp?.values?.length) {
        for (const cv of colorGrp.values) {
          if (cv.asin) colorAsinMap[cv.value] = cv.asin;
        }
      }
      // Also pull from comboAsin (color|size → asin)
      if (product.comboAsin) {
        for (const [key, asin] of Object.entries(product.comboAsin)) {
          const color = key.split('|')[0];
          if (color && asin && !colorAsinMap[color]) colorAsinMap[color] = asin;
        }
      }
      if (Object.keys(colorAsinMap).length) product.colorAsinMap = colorAsinMap;

      if (!product.title) {
        return res.json({ success: false, error: 'Amazon blocked the request (bot detection). Wait 30 seconds and try again, or paste the URL directly.' });
      }
      return res.json({ success: true, product, _debug: { pricesFound, imagesFound, totalColors: colorGrp?.values.length||0, colorAsinMapSize: Object.keys(colorAsinMap).length, pricesFailed: !!product._pricesFailed, pricesFailedReason: product._pricesFailedReason } });
    }

    // ── PUSH: create eBay listing ─────────────────────────────────────────────
    if (action === 'push') {
      return handlePush({ body, res, resolvePolicies, getCategories, aiEnrich, sanitizeTitle, ensureLocation, buildOffer, sleep, getEbayUrls });
    }

    // ── FETCH MY EBAY LISTINGS ────────────────────────────────────────────────
    if (action === 'fetchMyListings') {
      const { access_token } = body;
      if (!access_token) return res.status(400).json({ error: 'Missing access_token' });
      const EBAY_API = getEbayUrls(false).EBAY_API;
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };

      const allListings = [];
      const seenIds = new Set();

      // ── Strategy 1: Inventory API offers (catches DropSync-created listings) ──
      try {
        let offset = 0;
        const limit = 100;
        while (true) {
          const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=${limit}&offset=${offset}`, { headers: auth });
          const d = await r.json();
          if (!r.ok) { console.log('[fetchMyListings] inventory offers err:', d?.errors?.[0]?.message); break; }
          for (const o of (d.offers || [])) {
            if (!o.listingId || seenIds.has(o.listingId)) continue;
            seenIds.add(o.listingId);
            allListings.push({
              ebayListingId: o.listingId,
              ebaySku:       o.sku || '',
              title:         o.sku || '',
              price:         parseFloat(o.pricingSummary?.price?.value || 0),
              quantity:      o.availableQuantity || 0,
              image:         '',
              ebayUrl:       `https://www.ebay.com/itm/${o.listingId}`,
              aspects:       {},
            });
          }
          const total = d.total || 0;
          offset += limit;
          if (offset >= total || (d.offers||[]).length < limit) break;
        }
        console.log(`[fetchMyListings] inventory offers: ${allListings.length}`);
      } catch(e) { console.log('[fetchMyListings] inventory err:', e.message); }

      // ── Strategy 2: Trading API GetMyeBaySelling (catches ALL listings) ──────
      try {
        const TRADING_API = 'https://api.ebay.com/ws/api.dll';
        let pageNum = 1, totalPages = 1;
        while (pageNum <= totalPages && pageNum <= 20) {
          const xmlBody = `<?xml version="1.0" encoding="utf-8"?><GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents"><RequesterCredentials><eBayAuthToken>${access_token}</eBayAuthToken></RequesterCredentials><ActiveList><Include>true</Include><IncludeItemSpecifics>true</IncludeItemSpecifics><Pagination><EntriesPerPage>200</EntriesPerPage><PageNumber>${pageNum}</PageNumber></Pagination></ActiveList><ErrorLanguage>en_US</ErrorLanguage><WarningLevel>High</WarningLevel></GetMyeBaySellingRequest>`;
          const r = await fetch(TRADING_API, {
            method: 'POST',
            headers: {
              'Content-Type': 'text/xml',
              'X-EBAY-API-SITEID': '0',
              'X-EBAY-API-COMPATIBILITY-LEVEL': '1155',
              'X-EBAY-API-CALL-NAME': 'GetMyeBaySelling',
              'X-EBAY-API-IAF-TOKEN': access_token,
            },
            body: xmlBody,
          });
          const xml = await r.text();
          console.log(`[fetchMyListings] trading page=${pageNum} status=${r.status} len=${xml.length}`);
          if (!r.ok) break;

          // Check for eBay error in XML
          const ackMatch = xml.match(/<Ack>(.*?)<\/Ack>/);
          const ack = ackMatch?.[1] || '';
          if (ack === 'Failure') {
            const errMsg = xml.match(/<LongMessage>(.*?)<\/LongMessage>/)?.[1] || xml.slice(0,300);
            console.log('[fetchMyListings] trading API failure:', errMsg);
            break;
          }

          const tpMatch = xml.match(/<TotalNumberOfPages>(\d+)<\/TotalNumberOfPages>/);
          if (tpMatch) totalPages = Math.min(parseInt(tpMatch[1]), 20);

          // Parse items
          const itemRegex = /<Item>([\s\S]*?)<\/Item>/g;
          let count = 0;
          for (const m of xml.matchAll(itemRegex)) {
            const block = m[1];
            const get = tag => block.match(new RegExp(`<${tag}>([\s\S]*?)<\/${tag}>`))?.[1]?.trim() || '';
            const itemId = get('ItemID');
            if (!itemId || seenIds.has(itemId)) continue;
            seenIds.add(itemId);
            count++;
            allListings.push({
              ebayListingId: itemId,
              ebaySku:       get('SKU'),
              title:         get('Title'),
              price:         parseFloat(get('CurrentPrice') || get('BuyItNowPrice') || '0'),
              quantity:      parseInt(get('QuantityAvailable') || get('Quantity') || '0'),
              image:         get('GalleryURL') || get('PictureURL') || '',
              ebayUrl:       `https://www.ebay.com/itm/${itemId}`,
              aspects:       {},
            });
          }
          console.log(`[fetchMyListings] trading page ${pageNum}/${totalPages}: +${count} items`);
          pageNum++;
        }
      } catch(e) { console.log('[fetchMyListings] trading err:', e.message); }

      // ── Enrich titles/images via inventory items for DropSync SKUs ────────────
      const dsItems = allListings.filter(l => l.ebaySku?.startsWith('DS-'));
      if (dsItems.length) {
        const skus = [...new Set(dsItems.map(l => l.ebaySku))].slice(0, 100);
        for (let i = 0; i < skus.length; i += 25) {
          try {
            const batch = skus.slice(i, i+25);
            const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?sku=${batch.join('|')}`, { headers: auth });
            const id = await ir.json();
            for (const item of (id.inventoryItems || [])) {
              const l = allListings.find(x => x.ebaySku === item.sku);
              if (l) {
                if (!l.title && item.product?.title) l.title = item.product.title;
                if (!l.image && item.product?.imageUrls?.[0]) l.image = item.product.imageUrls[0];
              }
            }
          } catch {}
        }
      }

      console.log(`[fetchMyListings] TOTAL: ${allListings.length} listings`);
      return res.json({ success: true, listings: allListings, total: allListings.length });
    }


    // ── FETCH MY LISTINGS DEBUG — returns raw XML ─────────────────────────────
    if (action === 'fetchMyListingsDebug') {
      const { access_token } = body;
      if (!access_token) return res.status(400).json({ error: 'Missing access_token' });
      const TRADING_API = 'https://api.ebay.com/ws/api.dll';
      const xmlBody = `<?xml version="1.0" encoding="utf-8"?>
<GetMyeBaySellingRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <RequesterCredentials><eBayAuthToken>${access_token}</eBayAuthToken></RequesterCredentials>
  <ActiveList><Include>true</Include><Pagination><EntriesPerPage>10</EntriesPerPage><PageNumber>1</PageNumber></Pagination></ActiveList>
  <ErrorLanguage>en_US</ErrorLanguage><WarningLevel>High</WarningLevel>
</GetMyeBaySellingRequest>`;
      const r = await fetch(TRADING_API, {
        method: 'POST',
        headers: {
          'Content-Type': 'text/xml',
          'X-EBAY-API-SITEID': '0',
          'X-EBAY-API-COMPATIBILITY-LEVEL': '967',
          'X-EBAY-API-CALL-NAME': 'GetMyeBaySelling',
          'X-EBAY-API-IAF-TOKEN': access_token,
        },
        body: xmlBody,
      });
      const xmlText = await r.text();
      console.log('[fetchMyListingsDebug] status:', r.status, 'len:', xmlText.length);
      return res.json({ status: r.status, xml: xmlText.slice(0, 5000) });
    }



    // ── REVISE: update live listing images/price/stock from fresh Amazon scrape ─
    // Does NOT delete or re-publish — revises the inventory items in place
    if (action === 'revise') {
      return handleRevise({ body, res, getCategories, aiEnrich, sanitizeTitle, sleep, getEbayUrls });
    }

    // ── SYNC: same strategy as push/revise, keeps existing listing ID ────────────
    if (action === 'sync') {
      // Strategy: identical to revise — full inventory item PUT (title+desc+images+aspects+qty)
      // + group PUT + offer price update. Returns a diff summary of what changed.
      const { access_token, ebaySku, sourceUrl } = body;
      if (!access_token || !ebaySku || !sourceUrl) {
        return res.status(400).json({ error: 'Missing access_token, ebaySku, or sourceUrl' });
      }
      const sandbox    = body.sandbox === true || body.sandbox === 'true';
      const EBAY_API   = getEbayUrls(sandbox).EBAY_API;
      const markupPct  = parseFloat(body.markup ?? 0);
      const handling   = parseFloat(body.handlingCost ?? 2);
      const ebayFee    = 0.1335;
      const defaultQty = parseInt(body.quantity) || 1;
      const applyMk    = (cost) => {
        const c = parseFloat(cost) || 0;
        if (c <= 0) return 0;
        return Math.max(Math.ceil(((c + handling) * (1 + markupPct / 100) / (1 - ebayFee) + 0.30) * 100) / 100, 0.99);
      };
      const auth = {
        Authorization:      `Bearer ${access_token}`,
        'Content-Type':     'application/json',
        'Content-Language': 'en-US',
        'Accept-Language':  'en-US',
      };
      console.log(`[sync] sku=${ebaySku?.slice(0,35)} markup=${markupPct}%`);

      // ── STEP 1: Full Amazon scrape ────────────────────────────────────────────
      // Use client-provided HTML if available (bypasses Vercel IP blocking)
      const clientHtmlSync = body.clientHtml || null;
      let product = null;

      if (clientHtmlSync) {
        console.log('[sync] using client-provided HTML — skipping server-side fetch');
        product = await scrapeAmazonProduct(sourceUrl, clientHtmlSync).catch(() => null);
      } else {
        const baseUrl = process.env.VERCEL_URL
          ? `https://${process.env.VERCEL_URL}`
          : 'https://dropsync-one.vercel.app';
        const scrapeR = await fetch(`${baseUrl}/api/ebay?action=scrape`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ url: sourceUrl }),
        }).catch(e => { console.warn('[sync] scrape failed:', e.message); return null; });
        const scrapeD = scrapeR?.ok ? await scrapeR.json().catch(() => null) : null;
        product = scrapeD?.product || null;
      }

      // Fallback to cached frontend data if Amazon blocked
      const fallbackImages  = Array.isArray(body.fallbackImages) ? body.fallbackImages.filter(u => typeof u === 'string' && u.startsWith('http')) : [];
      const fallbackPrice   = parseFloat(body.fallbackPrice) || 0;
      const fallbackInStock = body.fallbackInStock !== false;
      const fallbackTitle   = typeof body.fallbackTitle === 'string' ? body.fallbackTitle : '';

      if (!product) {
        // Amazon blocked — use any cached data from frontend (images, price, title)
        if (fallbackTitle || fallbackPrice > 0 || fallbackImages.length) {
          console.log(`[sync] scrape blocked — using cached fallback: title="${fallbackTitle?.slice(0,40)}" imgs=${fallbackImages.length} price=$${fallbackPrice}`);
          product = {
            title: fallbackTitle || 'Product', price: fallbackPrice, images: fallbackImages,
            inStock: fallbackInStock, hasVariations: false, variations: [], variationImages: {},
            comboPrices: {}, sizePrices: {}, aspects: {}, breadcrumbs: [], bullets: [], descriptionPara: '',
          };
        }
      } else if (!product.images?.length && fallbackImages.length) {
        product.images = fallbackImages;
      }

      if (!product) {
        return res.status(400).json({ error: 'Amazon is blocking requests and no cached data is available. Open the product on the Import tab to refresh it first.' });
      }
      console.log(`[sync] scraped: "${product.title?.slice(0,50)}" imgs=${product.images.length} price=$${product.price} hasVar=${product.hasVariations}`);

      // ── STEP 2: AI category + title + aspects (same as push/revise) ─────────
      const suggestions = await getCategories(product.title || '', access_token).catch(() => []);
      const ai          = await aiEnrich(product.title, product.breadcrumbs || [], product.aspects || {}, suggestions).catch(() => null);
      const categoryId  = ai?.categoryId || suggestions[0]?.id || '11450';
      const rawTitle    = product.ebayTitle || ai?.title || product.title || 'Product';
      const listingTitle = sanitizeTitle(rawTitle) || sanitizeTitle(product.title) || 'Product';

      // ── STEP 3: Build description (same as push/revise) ──────────────────────
      const buildEbayDesc = (title, bullets, para, asp) => {
        const bulletHtml = (bullets || []).length
          ? '<ul>' + bullets.map(b => `<li>${String(b).replace(/</g,'&lt;').replace(/>/g,'&gt;')}</li>`).join('') + '</ul>' : '';
        const specRows = Object.entries(asp || {})
          .filter(([k,v]) => !['ASIN','UPC','Color','Size','Brand Name','Brand'].includes(k) && v[0] && String(v[0]).length < 80)
          .slice(0, 10).map(([k,v]) => `<tr><td><b>${k}</b></td><td>${v[0]}</td></tr>`).join('');
        const specsTable = specRows
          ? `<br/><table border="0" cellpadding="4" cellspacing="0" width="100%"><tbody>${specRows}</tbody></table>` : '';
        return [`<h2>${title}</h2>`, bulletHtml, para ? `<p>${para}</p>` : '', specsTable,
          '<br/><p style="font-size:11px;color:#888">Ships from US. Item is new. Please message us with any questions before purchasing.</p>',
        ].filter(Boolean).join('\n');
      };
      const ebayDescription = buildEbayDesc(listingTitle, product.bullets || [], product.descriptionPara || '', product.aspects || '')
                           || product.description || listingTitle;

      const aspects = { ...(product.aspects || {}), ...(ai?.aspects || {}) };
      delete aspects['Color']; delete aspects['color'];
      delete aspects['Size'];  delete aspects['size'];

      // Auto-fill required item specifics
      try {
        const catMeta = await fetch(
          `${EBAY_API}/commerce/taxonomy/v1/category_tree/0/get_item_aspects_for_category?category_id=${categoryId}`,
          { headers: { Authorization: `Bearer ${access_token}`, 'Accept-Language': 'en-US' } }
        ).then(r => r.json()).catch(() => ({}));
        for (const aspect of (catMeta.aspects || [])) {
          const name = aspect.aspectConstraint?.aspectRequired ? aspect.localizedAspectName : null;
          if (!name || aspects[name]) continue;
          const vals = (aspect.aspectValues || []).map(v => v.localizedValue);
          if (!vals.length) continue;
          const match = vals.find(v => (product.title || '').toLowerCase().includes(v.toLowerCase())) || vals[0];
          aspects[name] = [match];
        }
      } catch(e) { console.warn('[sync] aspects fetch failed:', e.message); }

      const freshStock  = product.inStock !== false;
      const colorGroup  = product.variations?.find(v => /color|colour/i.test(v.name));
      const sizeGroup   = product.variations?.find(v => /size/i.test(v.name));
      const colorImgs   = product.variationImages?.['Color'] || {};
      const comboPrices = product.comboPrices || {};
      const sizePrices  = product.sizePrices  || {};
      const comboAsin   = product.comboAsin   || {};
      const comboInStock = product.comboInStock || {};
      const basePrice   = parseFloat(product.price || 0);

      // Per-variant price — exact same logic as push/revise
      const getPriceForVariant = (color, size) => {
        const key = `${color||''}|${size||''}`;
        const cv  = colorGroup?.values?.find(v => v.value === color);
        const sv  = sizeGroup?.values?.find(v => v.value === size);
        const amazonPrice = comboPrices[key]
                         || sizePrices[size || '']
                         || parseFloat(cv?.price || sv?.price || basePrice || 0);
        const p = applyMk(amazonPrice);
        return p > 0 ? p : applyMk(basePrice) || 9.99;
      };

      // Per-variant qty — combo must exist AND be in stock on Amazon
      const getQtyForVariant = (color, size) => {
        if (!freshStock) return 0;
        if (Object.keys(comboAsin).length) {
          const key = `${color||''}|${size||''}`;
          if (!comboAsin[key]) return 0;
          // comboInStock[key] defaults to true if we couldn't check (unknown = assume in stock)
          if (comboInStock[key] === false) return 0;
          return defaultQty;
        }
        return defaultQty;
      };

      // Per-color image — same as push/revise
      const getImageForColor = (color) =>
        (color ? colorImgs[color] : null) || product.images[0] || '';

      console.log(`[sync] comboPrices=${Object.keys(comboPrices).length} sizePrices=${Object.keys(sizePrices).length} comboAsin=${Object.keys(comboAsin).length}`);

      // ── STEP 4: Get existing variant SKUs from eBay ──────────────────────────
      const groupRes    = await fetch(
        `${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`,
        { headers: auth }
      ).then(r => r.json()).catch(() => null);
      const variantSkus = groupRes?.variantSKUs || [];
      const isVariation = variantSkus.length > 0;
      console.log(`[sync] type=${isVariation?'variation':'simple'} variantSkus=${variantSkus.length}`);

      // Change tracking
      const priceChanges = [], stockChanges = [], imageChanges = [];

      if (isVariation) {
        // ── VARIATION: full inventory PUT per variant (same as push/revise) ────

        // Read Color+Size aspects for all existing variant SKUs from eBay
        const skuAspects = {};  // sku → { Color, Size }
        for (let i = 0; i < variantSkus.length; i += 20) {
          const batch = variantSkus.slice(i, i + 20);
          const qs    = batch.map(s => `sku=${encodeURIComponent(s)}`).join('&');
          const bd    = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?${qs}`, { headers: auth })
            .then(r => r.json()).catch(() => ({}));
          for (const item of (bd.inventoryItems || [])) {
            const asp = item.inventoryItem?.product?.aspects || {};
            skuAspects[item.sku] = {
              Color: (asp['Color'] || asp['color'] || [])[0] || null,
              Size:  (asp['Size']  || asp['size']  || [])[0] || null,
            };
          }
          if (i + 20 < variantSkus.length) await sleep(100);
        }
        console.log(`[sync] aspects read: ${Object.keys(skuAspects).length}/${variantSkus.length}`);

        // Full inventory PUT per variant — same as push/revise
        // title + description + imageUrls (per-color) + aspects + qty
        const createdSkus = new Set();
        const failedSkus  = [];

        // Test first variant for 401
        if (variantSkus.length > 0) {
          const testSku = variantSkus[0];
          const { Color: testColor, Size: testSize } = skuAspects[testSku] || {};
          const testAsp = { ...aspects };
          if (testColor) testAsp['Color'] = [testColor];
          if (testSize)  testAsp['Size']  = [testSize];
          const testImg = getImageForColor(testColor);

          // Track old qty for stock change detection
          const oldItem = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(testSku)}`, { headers: auth })
            .then(r => r.ok ? r.json() : {}).catch(() => ({}));
          const oldQty  = oldItem?.availability?.shipToLocationAvailability?.quantity ?? -1;
          const newQty  = getQtyForVariant(testColor, testSize);

          const testR = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(testSku)}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({
              availability: { shipToLocationAvailability: { quantity: newQty } },
              condition: 'NEW',
              product: {
                title:       listingTitle,
                description: ebayDescription,
                imageUrls:   [testImg, ...product.images.filter(x => x !== testImg)].filter(Boolean).slice(0, 12),
                aspects:     testAsp,
              },
            }),
          });
          if (testR.status === 401) {
            return res.status(401).json({ error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect (fix 401) and re-authorize.', code: 'INVENTORY_401' });
          }
          if (testR.ok || testR.status === 204) {
            createdSkus.add(testSku);
            if (oldQty >= 0 && oldQty !== newQty) {
              const label = [testColor, testSize].filter(Boolean).join('/') || testSku.slice(-12);
              stockChanges.push(`${label}: qty ${oldQty}→${newQty}`);
              console.log(`[sync] stock ${label}: ${oldQty}→${newQty}`);
            }
          } else {
            const t = await testR.text();
            console.warn(`[sync] test PUT fail: ${testR.status} ${t.slice(0,100)}`);
            failedSkus.push(testSku);
          }
        }

        // Remaining variants in batches of 8 (same as push/revise)
        for (let i = 1; i < variantSkus.length; i += 8) {
          await Promise.all(variantSkus.slice(i, i + 8).map(async (sku) => {
            const { Color: color, Size: size } = skuAspects[sku] || {};
            const asp = { ...aspects };
            if (color) asp['Color'] = [color];
            if (size)  asp['Size']  = [size];
            const img    = getImageForColor(color);
            const newQty = getQtyForVariant(color, size);

            // Track old qty
            const oldItem = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, { headers: auth })
              .then(r => r.ok ? r.json() : {}).catch(() => ({}));
            const oldQty = oldItem?.availability?.shipToLocationAvailability?.quantity ?? -1;

            const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
              method: 'PUT', headers: auth,
              body: JSON.stringify({
                availability: { shipToLocationAvailability: { quantity: newQty } },
                condition: 'NEW',
                product: {
                  title:       listingTitle,
                  description: ebayDescription,
                  imageUrls:   img ? [img] : product.images.slice(0, 1),
                  aspects:     asp,
                },
              }),
            });
            if (r.ok || r.status === 204) {
              createdSkus.add(sku);
              if (oldQty >= 0 && oldQty !== newQty) {
                const label = [color, size].filter(Boolean).join('/') || sku.slice(-12);
                stockChanges.push(`${label}: qty ${oldQty}→${newQty}`);
              }
            } else {
              const t = await r.text();
              console.warn(`[sync] PUT fail ${sku.slice(-20)}: ${r.status} ${t.slice(0,80)}`);
              failedSkus.push(sku);
            }
          }));
          if (i + 8 < variantSkus.length) await sleep(150);
        }

        // Retry failed items once
        if (failedSkus.length) {
          await sleep(800);
          for (const sku of failedSkus) {
            const { Color: color, Size: size } = skuAspects[sku] || {};
            const asp = { ...aspects }; if (color) asp['Color'] = [color]; if (size) asp['Size'] = [size];
            const img    = getImageForColor(color);
            const newQty = getQtyForVariant(color, size);
            const r = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
              method: 'PUT', headers: auth,
              body: JSON.stringify({
                availability: { shipToLocationAvailability: { quantity: newQty } },
                condition: 'NEW',
                product: { title: listingTitle, description: ebayDescription,
                           imageUrls: [img, ...product.images.filter(x => x !== img)].filter(Boolean).slice(0, 12), aspects: asp },
              }),
            });
            if (r.ok || r.status === 204) createdSkus.add(sku);
          }
        }
        console.log(`[sync] inventory items: ${createdSkus.size}/${variantSkus.length} updated, ${failedSkus.length} failed`);

        // ── GROUP PUT: fresh images + variesBy (same as push/revise) ───────────
        const varAspects = {};
        if (colorGroup) varAspects['Color'] = colorGroup.values.map(v => v.value);
        if (sizeGroup)  varAspects['Size']  = sizeGroup.values.map(v => v.value);
        const colorUrlList   = Object.values(colorImgs).filter(Boolean).slice(0, 12);
        const groupImageUrls = colorUrlList.length ? colorUrlList : product.images.slice(0, 12);
        const groupAspects   = { ...aspects };
        for (const vg of (product.variations || [])) { delete groupAspects[vg.name]; delete groupAspects[vg.name.toLowerCase()]; }
        const variesBySpecs  = Object.entries(varAspects).map(([name, values]) => ({ name, values }));

        // Track group image changes
        const prevGroupImages = groupRes?.imageUrls || [];
        if (prevGroupImages.length !== groupImageUrls.length) {
          imageChanges.push(`Gallery: ${prevGroupImages.length}→${groupImageUrls.length} images`);
        }

        for (let attempt = 1; attempt <= 3; attempt++) {
          const gr = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, {
            method: 'PUT', headers: auth,
            body: JSON.stringify({
              inventoryItemGroupKey: ebaySku,
              title:       listingTitle,
              description: ebayDescription,
              imageUrls:   groupImageUrls,
              variantSKUs: variantSkus.filter(s => createdSkus.has(s)),
              aspects:     groupAspects,
              variesBy: {
                aspectsImageVariesBy: colorGroup ? ['Color'] : [],
                specifications: variesBySpecs,
              },
            }),
          });
          if (gr.ok || gr.status === 204) { console.log('[sync] group PUT ok'); break; }
          const gt = await gr.text();
          console.warn(`[sync] group attempt ${attempt}: ${gr.status} ${gt.slice(0,200)}`);
          if (attempt < 3) await sleep(600);
        }

        // ── UPDATE OFFER PRICES: per-variant with correct color+size price ───
        let pricesUpdated = 0;
        for (let i = 0; i < variantSkus.length; i += 15) {
          await Promise.all(variantSkus.slice(i, i + 8).map(async (sku) => {
            if (!createdSkus.has(sku)) return;
            const { Color: color, Size: size } = skuAspects[sku] || {};
            const newPrice = getPriceForVariant(color, size);
            const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth })
              .then(r => r.json()).catch(() => ({}));
            const offer = (ol.offers || [])[0];
            if (offer?.offerId) {
              const oldPrice = parseFloat(offer.pricingSummary?.price?.value || 0);
              if (Math.abs(newPrice - oldPrice) > 0.01) {
                await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offer.offerId}`, {
                  method: 'PUT', headers: auth,
                  body: JSON.stringify({ pricingSummary: { price: { value: newPrice.toFixed(2), currency: 'USD' } } }),
                }).catch(() => {});
                const label = [color, size].filter(Boolean).join('/') || sku.slice(-12);
                priceChanges.push(`${label}: $${oldPrice.toFixed(2)}→$${newPrice.toFixed(2)}`);
                console.log(`[sync] price ${label}: $${oldPrice.toFixed(2)}→$${newPrice.toFixed(2)}`);
              }
              pricesUpdated++;
            }
          }));
          if (i + 15 < variantSkus.length) await sleep(80);
        }

        console.log(`[sync] done — ${createdSkus.size} updated, ${pricesUpdated} prices checked, ${priceChanges.length} price changes, ${stockChanges.length} stock changes`);
        return res.json({
          success:      true,
          type:         'variation',
          updatedVariants: createdSkus.size,
          failed:       failedSkus.length,
          images:       product.images.length,
          price:        applyMk(basePrice),
          inStock:      freshStock,
          priceChanges,
          stockChanges,
          imageChanges,
        });

      } else {
        // ── SIMPLE LISTING: full inventory PUT (same as push/revise) ──────────
        const newPrice = applyMk(product.price) || 9.99;
        const newQty   = freshStock ? defaultQty : 0;

        // Track old values
        const oldItem  = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, { headers: auth })
          .then(r => r.ok ? r.json() : {}).catch(() => ({}));
        const oldQty   = oldItem?.availability?.shipToLocationAvailability?.quantity ?? -1;
        const oldImgCount = (oldItem?.product?.imageUrls || []).length;

        const ir = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, {
          method: 'PUT', headers: auth,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: newQty } },
            condition: 'NEW',
            product: { title: listingTitle, description: ebayDescription, imageUrls: product.images.slice(0, 12), aspects },
          }),
        });
        if (ir.status === 401) {
          return res.status(401).json({ error: 'eBay token missing inventory permission. Go to Settings → Force Reconnect (fix 401) and re-authorize.', code: 'INVENTORY_401' });
        }
        if (!ir.ok) {
          const t = await ir.text();
          return res.status(400).json({ error: `Inventory update failed: ${t.slice(0,200)}` });
        }

        if (oldQty >= 0 && oldQty !== newQty) {
          stockChanges.push(`qty ${oldQty}→${newQty}`);
          console.log(`[sync/simple] stock: ${oldQty}→${newQty}`);
        }
        if (oldImgCount !== product.images.length) {
          imageChanges.push(`Images: ${oldImgCount}→${product.images.length}`);
        }

        // Update offer price
        const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(ebaySku)}`, { headers: auth })
          .then(r => r.json()).catch(() => ({}));
        const offer = (ol.offers || [])[0];
        if (offer?.offerId) {
          const oldPrice = parseFloat(offer.pricingSummary?.price?.value || 0);
          if (Math.abs(newPrice - oldPrice) > 0.01) {
            await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offer.offerId}`, {
              method: 'PUT', headers: auth,
              body: JSON.stringify({ pricingSummary: { price: { value: newPrice.toFixed(2), currency: 'USD' } } }),
            }).catch(() => {});
            priceChanges.push(`$${oldPrice.toFixed(2)} → $${newPrice.toFixed(2)}`);
            console.log(`[sync/simple] price: $${oldPrice.toFixed(2)} → $${newPrice.toFixed(2)}`);
          }
        }

        console.log(`[sync/simple] title="${listingTitle.slice(0,40)}" price=$${newPrice} qty=${newQty} imgs=${product.images.length}`);
        return res.json({
          success:      true,
          type:         'simple',
          updatedVariants: 1,
          images:       product.images.length,
          price:        newPrice,
          inStock:      freshStock,
          priceChanges,
          stockChanges,
          imageChanges,
        });
      }
    }

    // ── END LISTING ───────────────────────────────────────────────────────────
    // ── DEALS SCRAPE v2: get ASINs from Amazon Today's Deals page ────────────────
    if (action === 'bulkScrapeAsins') action = 'dealsScrape'; // backward compat alias
    if (action === 'dealsScrape') {
      const { exclude = [], count = 25 } = body;
      const excludeSet = new Set(exclude);

      // VeRO / brand-protected items to skip (eBay will reject these)
      const VERO_BRANDS = new Set([
        'samsung','apple','nike','adidas','sony','microsoft','lg','bose',
        'beats','dyson','lego','gucci','louis vuitton','chanel','rolex',
        'prada','versace','burberry','yeezy','jordan','off-white','supreme',
        'north face','ugg','timberland','new balance','under armour','reebok',
        'puma','vans','converse','dr. martens','crocs','birkenstock',
        'kindle','echo','ring','roomba','nespresso','keurig','instant pot',
        'nintendo','playstation','xbox','fitbit','garmin','gopro',
        'turbotax','doordash','uber','instacart','grubhub',
        'pokemon','disney','marvel','dc comics','star wars','harry potter',
      ]);

      // Digital / non-physical product keywords to skip
      const SKIP_KEYWORDS = [
        'ebook','kindle edition','gift card','egift','digital code',
        'game download','app purchase','audible','prime video',
        'rolling balls','brain games','alpha\'s bride','design home',
        'iq boost','training brain','free 3d game',
      ];

      const isVero = (title, brand) => {
        const t = (title + ' ' + (brand||'')).toLowerCase();
        for (const b of VERO_BRANDS) if (t.includes(b)) return true;
        for (const k of SKIP_KEYWORDS) if (t.includes(k)) return true;
        return false;
      };

      // Scrape multiple Amazon pages to get a large, diverse ASIN pool
      const AMAZON_PAGES = [
        'https://www.amazon.com/deals?ref_=nav_cs_gb',
        'https://www.amazon.com/s?i=fashion&rh=n%3A7141123011&fs=true&ref=lp_7141123011_sar',
        'https://www.amazon.com/s?i=fashion-womens&bbn=1040660&rh=n%3A1040660%2Cn%3A7147441011&ref=nav_em',
        'https://www.amazon.com/s?k=womens+clothing&i=fashion-womens&ref=nb_sb_noss',
        'https://www.amazon.com/s?k=home+decor&i=garden&ref=nb_sb_noss',
        'https://www.amazon.com/s?k=kitchen+gadgets&i=kitchen&ref=nb_sb_noss',
        'https://www.amazon.com/s?k=jewelry+women&i=jewelry&ref=nb_sb_noss',
        'https://www.amazon.com/s?k=baby+products&i=baby-products&ref=nb_sb_noss',
        'https://www.amazon.com/s?k=sports+fitness&i=sporting&ref=nb_sb_noss',
        'https://www.amazon.com/gp/bestsellers/fashion/ref=zg_bs_nav_fashion_0',
        'https://www.amazon.com/gp/bestsellers/garden/ref=zg_bs_nav_garden_0',
        'https://www.amazon.com/gp/new-releases/fashion/ref=zg_bsnr_nav_fashion_0',
      ];

      const productMap = {}; // asin → { asin, title, url }
      let pagesLoaded = 0;

      // Fetch pages in parallel (up to 4 at a time) until we have enough ASINs
      const fetchPage_ = async (url) => {
        try {
          const h = await fetchPage(url, randUA());
          if (!h || h.length < 5000) return;
          // Extract ASINs using all patterns
          for (const m of h.matchAll(/"asin"\s*:\s*"([B][0-9A-Z]{9})"/g))
            productMap[m[1]] = productMap[m[1]] || { asin: m[1], title: '' };
          for (const m of h.matchAll(/data-asin="([B][0-9A-Z]{9})"/g))
            productMap[m[1]] = productMap[m[1]] || { asin: m[1], title: '' };
          for (const m of h.matchAll(/href="[^"]*\/([^\/\s"]{5,})\/dp\/([B][0-9A-Z]{9})[^"]*"/g)) {
            const slug = decodeURIComponent(m[1]).replace(/-/g,' ');
            if (slug.length > 5) productMap[m[2]] = { asin: m[2], title: slug, url: 'https://www.amazon.com/dp/'+m[2] };
          }
          for (const m of h.matchAll(/\/dp\/([B][0-9A-Z]{9})[^"]*"[^>]*>[\s\S]{0,300}?alt="([^"]{10,120})"/g)) {
            if (!productMap[m[1]]?.title || productMap[m[1]].title.length < 10)
              productMap[m[1]] = { asin: m[1], title: m[2], url: 'https://www.amazon.com/dp/'+m[1] };
          }
          pagesLoaded++;
          console.log('[dealsScrape] loaded page', url.slice(0,60), '→ pool='+Object.keys(productMap).length);
        } catch(e) { console.error('[dealsScrape] page error:', e.message); }
      };

      // Always fetch deals page + rotate through category pages based on exclude set size
      // so every "Fetch More" call hits different pages
      const pageOffset = Math.floor(excludeSet.size / 20) % AMAZON_PAGES.length;
      const pagesToFetch = [
        AMAZON_PAGES[0], // deals always
        ...AMAZON_PAGES.slice(1).sort(() => Math.random()-0.5).slice(0, 4)
      ];

      // Fetch in batches of 2 (avoid hammering)
      for (let i = 0; i < pagesToFetch.length; i += 2) {
        await Promise.all(pagesToFetch.slice(i, i+2).map(fetchPage_));
        if (Object.keys(productMap).length >= excludeSet.size + count + 50) break;
        if (i+2 < pagesToFetch.length) await new Promise(r => setTimeout(r, 600));
      }

      if (Object.keys(productMap).length === 0)
        return res.json({ success: false, error: 'Could not load Amazon pages. Try again in a moment.' });

      console.log('[dealsScrape] total pool after', pagesLoaded, 'pages:', Object.keys(productMap).length);
      // Filter: exclude already-used, VeRO, and digital items
      const allProducts = Object.values(productMap).filter(p =>
        p.asin &&
        !excludeSet.has(p.asin) &&
        !isVero(p.title, '')
        // note: title may be empty — that's OK, we'll get it when we scrape
      );

      // Shuffle randomly
      for (let i = allProducts.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [allProducts[i], allProducts[j]] = [allProducts[j], allProducts[i]];
      }

      const selected = allProducts.slice(0, count);
      console.log(`[dealsScrape] total=${Object.keys(productMap).length} after filter=${allProducts.length} selected=${selected.length}`);
      return res.json({ success: true, products: selected, totalFound: Object.keys(productMap).length });
    }

    if (action === 'optimizeTitle') {
      const { title, breadcrumbs = [], aspects = {} } = body;
      if (!title) return res.status(400).json({ error: 'title required' });
      const key = process.env.ANTHROPIC_API_KEY;
      if (!key) return res.status(500).json({ error: 'No ANTHROPIC_API_KEY set' });
      try {
        const r = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-api-key': key, 'anthropic-version': '2023-06-01' },
          body: JSON.stringify({
            model: 'claude-sonnet-4-20250514', max_tokens: 200,
            messages: [{ role: 'user', content: `You are an eBay listing title optimizer.

Original Amazon title: "${title}"
Category path: ${breadcrumbs.join(' > ') || 'unknown'}

Generate a clean, optimized eBay listing title following these rules:
- Exactly 10-12 words
- Remove ALL brand names, seller names, and store names completely
- Start with the product type (e.g. "Chunky Knit Throw Blanket", "Women's V-Neck T-Shirt")
- Include key descriptors: material, style, color range hint, use case
- No special characters, no colons, no quotes, no dimensions in title
- No ALL CAPS words, no filler words
- NEVER use: authentic, genuine, original, real, verified, certified — eBay bans these
- Title case formatting

Return ONLY the optimized title text, nothing else. No quotes, no explanation.` }]
          })
        });
        const d = await r.json();
        const raw = (d.content?.[0]?.text || '').trim().replace(/^["']|["']$/g, '');
        const optimized = sanitizeTitle(raw);
        console.log(`[optimizeTitle] raw="${raw}" cleaned="${optimized}" apiErr=${JSON.stringify(d.error)}`);
        if (!optimized) {
          // AI returned empty — fall back to cleaning up the raw Amazon title
          const fallback = sanitizeTitle(title.replace(/[^\w\s\-&]/g, ' ').replace(/\s+/g, ' ').trim().slice(0, 80));
          return res.json({ success: true, optimizedTitle: fallback, fallback: true });
        }
        return res.json({ success: true, optimizedTitle: optimized });
      } catch (e) {
        return res.status(500).json({ error: e.message });
      }
    }

    if (action === 'endListing') {
      const { access_token, ebaySku, ebayListingId } = body;
      const sandbox = body.sandbox === true || body.sandbox === 'true';
      const EBAY_API = getEbayUrls(sandbox).EBAY_API;
      const auth = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };
      let ended = 0;
      const variantSkus = [];

      // Strategy A: get group to find all variant SKUs
      const groupRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
      if (groupRes.variantSKUs?.length) variantSkus.push(...groupRes.variantSKUs);
      console.log(`[end] group=${ebaySku} variantSkus=${variantSkus.length}`);

      // Strategy B: if group returned no SKUs, page through all offers and find ones
      // belonging to this listing via ebayListingId
      if (!variantSkus.length) {
        console.log('[end] group empty — scanning offers by listing ID:', ebayListingId);
        let offset = 0;
        while (true) {
          const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=100&offset=${offset}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
          const offers = ol.offers || [];
          if (!offers.length) break;
          for (const o of offers) {
            // Match by listing ID or by SKU prefix
            const matchesListing = ebayListingId && o.listing?.listingId === String(ebayListingId);
            const matchesSku = o.sku?.startsWith(ebaySku.split('-').slice(0,3).join('-'));
            if (matchesListing || matchesSku) {
              if (!variantSkus.includes(o.sku)) variantSkus.push(o.sku);
            }
          }
          if (offers.length < 100) break;
          offset += 100;
        }
        console.log(`[end] scan found ${variantSkus.length} variant skus`);
      }

      // Withdraw + delete all offers for each variant SKU
      const allSkus = variantSkus.length ? variantSkus : [ebaySku];
      for (const sku of allSkus) {
        const ol = await fetch(`${EBAY_API}/sell/inventory/v1/offer?sku=${encodeURIComponent(sku)}`, { headers: auth }).then(r=>r.json()).catch(()=>({}));
        for (const offer of (ol.offers||[])) {
          if (offer.status === 'PUBLISHED') {
            const wr = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offer.offerId}/withdraw`, { method: 'POST', headers: auth });
            const wb = await wr.json().catch(()=>({}));
            console.log(`[end] withdraw offer ${offer.offerId}:`, wr.status, JSON.stringify(wb).slice(0,100));
            if (wr.status < 300) ended++;
          }
          await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offer.offerId}`, { method: 'DELETE', headers: auth }).catch(()=>{});
        }
      }

      // Delete inventory item group
      await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, { method: 'DELETE', headers: auth }).catch(()=>{});

      // Delete all variant inventory items in batches of 25
      for (let i = 0; i < variantSkus.length; i += 25) {
        const batch = variantSkus.slice(i, i+25);
        await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item?sku=${batch.map(encodeURIComponent).join(',')}`, { method: 'DELETE', headers: auth }).catch(()=>{});
      }

      console.log(`[end] done ended=${ended} deletedItems=${variantSkus.length}`);
      return res.json({ success: true, ended, deleted: variantSkus.length });
    }

    if (action === 'getOrders') {
      // FIX: frontend calls this as GET — read token from query params too, not just body
      const access_token = body.access_token || req.query.access_token;
      const limit = body.limit || req.query.limit || 50;
      if (!access_token) return res.status(400).json({ error: 'Missing access_token' });
      const fromDate = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString();
      // FIX: encode [ ] brackets in filter — unencoded brackets cause fetch to throw on some Node versions
      const filter = encodeURIComponent(`lastmodifieddate:[${fromDate}..]`);
      const r = await fetch(
        `https://api.ebay.com/sell/fulfillment/v1/order?limit=${limit}&filter=${filter}`,
        { headers: { 'Authorization': `Bearer ${access_token}`, 'Content-Type': 'application/json' } }
      );
      // FIX: check r.ok BEFORE calling r.json() — eBay can return non-JSON on errors
      const text = await r.text();
      let d;
      try { d = JSON.parse(text); } catch { return res.status(r.status || 500).json({ error: text.slice(0, 300) }); }
      if (!r.ok) return res.status(r.status).json({ error: d.errors?.[0]?.message || JSON.stringify(d).slice(0, 300) });
      return res.json({ orders: d.orders || [], total: d.total || 0 });
    }

    // ── REPLENISH — wipe listing content, re-scrape Amazon, update in-place ─────
    // Unlike revise (which merges), replenish CLEARS everything first then rewrites
    // from scratch using fresh Amazon data. Same listing ID preserved. Rolls back on fail.
    if (action === 'replenish') {
      const { access_token, ebaySku, sourceUrl, markup, handlingCost, quantity, sandbox: sb } = body;
      const markupReplenish   = parseFloat(markup ?? 0);
      const handlingReplenish = parseFloat(handlingCost ?? 2);
      const defaultQtyR       = parseInt(quantity) || 1;
      const sandboxR          = sb === true || sb === 'true';
      const EBAY_API_R        = getEbayUrls(sandboxR).EBAY_API;
      const authR             = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Content-Language': 'en-US', 'Accept-Language': 'en-US' };

      if (!access_token || !ebaySku || !sourceUrl)
        return res.status(400).json({ error: 'Missing access_token, ebaySku, or sourceUrl' });

      const applyMkR = (cost) => {
        const c = parseFloat(cost) || 0;
        if (c <= 0) return 0;
        return Math.max(Math.ceil(((c + handlingReplenish) * (1 + markupReplenish / 100) / (1 - 0.1335) + 0.30) * 100) / 100, 0.99);
      };

      console.log(`[replenish] sku=${ebaySku?.slice(0,30)}`);

      // ── STEP 1: Snapshot existing eBay inventory (for rollback) ──────────
      let snapshot = null;
      let snapshotGroup = null;
      let variantSkusR = [];
      try {
        const grpSnap = await fetch(`${EBAY_API_R}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(ebaySku)}`, { headers: authR }).then(r => r.ok ? r.json() : null).catch(() => null);
        if (grpSnap) {
          snapshotGroup = grpSnap;
          variantSkusR = grpSnap.variantSKUs || [];
          // Snapshot each variant
          const snapItems = {};
          for (let i = 0; i < variantSkusR.length; i += 20) {
            const qs = variantSkusR.slice(i, i+20).map(s => `sku=${encodeURIComponent(s)}`).join('&');
            const bd = await fetch(`${EBAY_API_R}/sell/inventory/v1/inventory_item?${qs}`, { headers: authR }).then(r => r.json()).catch(() => ({}));
            for (const inv of (bd.inventoryItems || [])) snapItems[inv.sku] = inv.inventoryItem;
          }
          snapshot = { type: 'variation', group: grpSnap, items: snapItems };
        } else {
          const simpleSnap = await fetch(`${EBAY_API_R}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, { headers: authR }).then(r => r.ok ? r.json() : null).catch(() => null);
          if (simpleSnap) snapshot = { type: 'simple', item: simpleSnap };
        }
      } catch(e) { console.warn('[replenish] snapshot failed:', e.message); }

      // ── STEP 2: Wipe eBay listing content (qty=0, blank images/title/desc) ─
      // This clears stale data before writing fresh — same listing ID kept
      try {
        if (snapshot?.type === 'variation' && variantSkusR.length) {
          // Zero out all variant quantities
          for (let i = 0; i < variantSkusR.length; i += 15) {
            await Promise.all(variantSkusR.slice(i, i+15).map(async sku => {
              const existing = snapshot.items?.[sku] || {};
              await fetch(`${EBAY_API_R}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
                method: 'PUT', headers: authR,
                body: JSON.stringify({ ...existing, availability: { shipToLocationAvailability: { quantity: 0 } } }),
              }).catch(() => {});
            }));
          }
        } else if (snapshot?.type === 'simple') {
          const existing = snapshot.item || {};
          await fetch(`${EBAY_API_R}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, {
            method: 'PUT', headers: authR,
            body: JSON.stringify({ ...existing, product: { ...existing.product, title: existing.product?.title || 'Replenishing…', imageUrls: [], description: '' }, availability: { shipToLocationAvailability: { quantity: 0 } } }),
          }).catch(() => {});
        }
        console.log('[replenish] wiped existing content');
      } catch(e) { console.warn('[replenish] wipe error:', e.message); }

      // ── STEP 3: Fresh Amazon scrape ───────────────────────────────────────
      // Use browser-provided HTML if available (bypasses Vercel IP blocking)
      let freshProduct = null;
      const clientHtmlReplenish = body.clientHtml || null;

      if (clientHtmlReplenish) {
        console.log('[replenish] using client-provided HTML — no server-side Amazon fetch needed');
        freshProduct = await scrapeAmazonProduct(sourceUrl, clientHtmlReplenish);
      } else {
        try {
          const baseUrl = process.env.VERCEL_URL ? `https://${process.env.VERCEL_URL}` : 'https://dropsync-one.vercel.app';
          const scrapeR = await fetch(`${baseUrl}/api/ebay?action=scrape`, {
            method: 'POST', headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ url: sourceUrl }),
          }).catch(() => null);
          const scrapeD = scrapeR?.ok ? await scrapeR.json().catch(() => null) : null;
          freshProduct = scrapeD?.product || null;
        } catch(e) { console.warn('[replenish] scrape error:', e.message); }
      }

      // ── STEP 4: Validate scraped data — adapted to listing type ─────────────
      // Handles: simple listing, single-dim variation, two-dim variation,
      //          child ASIN (psc=1 redirect), bundle, blocked scrape
      const replenishErrors = [];

      if (!freshProduct) {
        replenishErrors.push('Amazon scrape failed — page blocked or invalid URL');
      } else {
        // ── 4a. Title ────────────────────────────────────────────────────────
        if (!freshProduct.title || freshProduct.title.trim().length < 3)
          replenishErrors.push('No title returned from Amazon — page may be blocked');

        // ── 4b. Images ───────────────────────────────────────────────────────
        if (!freshProduct.images?.length)
          replenishErrors.push('No images returned from Amazon');

        // ── 4c. Detect listing type and validate accordingly ─────────────────
        const isVariation   = freshProduct.hasVariations && freshProduct.variations?.length > 0;
        const hasComboAsin  = Object.keys(freshProduct.comboAsin  || {}).length > 0;
        const hasComboPrice = Object.keys(freshProduct.comboPrices || {}).length > 0;
        const hasSizePrice  = Object.keys(freshProduct.sizePrices  || {}).length > 0;

        if (isVariation) {
          // ── Variation listing (parent ASIN with child ASINs) ──────────────
          const primaryGroup   = freshProduct.variations[0];
          const secondaryGroup = freshProduct.variations[1] || null;
          const primaryCount   = primaryGroup?.values?.length  || 0;
          const secondaryCount = secondaryGroup?.values?.length || 0;

          console.log(`[replenish/validate] variation: dims=${freshProduct.variations.length} primary=${primaryCount} secondary=${secondaryCount} combos=${Object.keys(freshProduct.comboAsin||{}).length} pricesFailed=${freshProduct._pricesFailed}`);

          // Must have at least 1 dimension with at least 1 value
          if (primaryCount === 0)
            replenishErrors.push('Variation listing has no dimension values — scrape incomplete');

          // Must have combo ASIN mapping (confirms Amazon returned child ASINs)
          if (!hasComboAsin)
            replenishErrors.push('Variation listing missing child ASIN map — Amazon may have blocked variant fetch');

          // Must have at least some pricing (comboPrices or sizePrices)
          if (!hasComboPrice && !hasSizePrice && !freshProduct.price)
            replenishErrors.push('Variation listing: no prices found for any variant');

          // If prices failed (all identical = blocked), block replenish
          if (freshProduct._pricesFailed) {
            // Only block if no manual price can be used as fallback
            if (!freshProduct.price || freshProduct.price <= 0)
              replenishErrors.push(`Variation pricing unreliable — Amazon blocked per-variant price fetches and no base price available`);
            else
              console.warn(`[replenish/validate] _pricesFailed but base price $${freshProduct.price} available — allowing with warning`);
          }

          // Quantity validation for variations:
          // At least ONE combo must be in-stock. All-OOS = Amazon pulled the listing.
          const comboInStockR = freshProduct.comboInStock || {};
          const hasComboStock = Object.keys(comboInStockR).length > 0;
          if (hasComboStock) {
            const anyInStock = Object.values(comboInStockR).some(v => v !== false);
            if (!anyInStock)
              replenishErrors.push('All variants are out of stock on Amazon — replenish blocked to avoid dead listing');
          } else if (!freshProduct.inStock) {
            // No per-combo data but product-level says OOS
            replenishErrors.push('Product is out of stock on Amazon (no stock data for any variant)');
          }

        } else {
          // ── Simple listing or child ASIN (single item) ────────────────────
          // Child ASIN: url had ?psc=1 or /dp/CHILDASIN — scraper normalizes it
          // These have no variations, just a direct price + stock

          if (!freshProduct.price || freshProduct.price <= 0)
            replenishErrors.push('No price found — product may be unavailable or a bundle without a buy box');

          // Quantity check: product must be in stock
          // inStock=false means buy-box explicitly says "Currently unavailable"
          if (freshProduct.inStock === false)
            replenishErrors.push('Product is out of stock on Amazon — replenish blocked to avoid dead listing');

          // Extra: if it looks like a parent ASIN that returned no variations
          // (title exists, price=0, no variations) — this is a blocked/redirect scrape
          if (freshProduct.price <= 0 && freshProduct.title?.length > 3)
            replenishErrors.push('Price is $0 — this may be a parent ASIN redirect. Use the child ASIN URL (?th=1&psc=1)');
        }

        // ── 4d. ASIN sanity check ─────────────────────────────────────────────
        // If the scraped ASIN doesn't match the sourceUrl ASIN, we got redirected
        const urlAsin    = (sourceUrl.match(/\/dp\/([A-Z0-9]{10})/i) || [])[1] || '';
        const scrapedAsin = freshProduct.asin || '';
        if (urlAsin && scrapedAsin && urlAsin !== scrapedAsin) {
          // Amazon redirected to a different ASIN (common with parent→child redirect)
          // This is fine for child ASINs — log but don't block
          console.warn(`[replenish/validate] ASIN mismatch: url=${urlAsin} scraped=${scrapedAsin} — likely parent→child redirect, proceeding`);
        }
      }

      console.log(`[replenish/validate] errors=${replenishErrors.length}: ${replenishErrors.join(' | ') || 'none'}`);

      if (replenishErrors.length > 0) {
        // ── ROLLBACK: restore the snapshot we took ───────────────────────
        console.warn('[replenish] validation failed — rolling back:', replenishErrors.join(', '));
        try {
          if (snapshot?.type === 'variation' && variantSkusR.length) {
            for (let i = 0; i < variantSkusR.length; i += 15) {
              await Promise.all(variantSkusR.slice(i, i+15).map(sku =>
                fetch(`${EBAY_API_R}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
                  method: 'PUT', headers: authR, body: JSON.stringify(snapshot.items?.[sku] || {}),
                }).catch(() => {})
              ));
            }
          } else if (snapshot?.type === 'simple') {
            await fetch(`${EBAY_API_R}/sell/inventory/v1/inventory_item/${encodeURIComponent(ebaySku)}`, {
              method: 'PUT', headers: authR, body: JSON.stringify(snapshot.item),
            }).catch(() => {});
          }
          console.log('[replenish] rollback complete');
        } catch(re) { console.warn('[replenish] rollback error:', re.message); }
        return res.status(422).json({ success: false, rolledBack: true, errors: replenishErrors, message: `Replenish failed — old listing restored. Reason: ${replenishErrors[0]}` });
      }

      // ── STEP 5: Re-apply full fresh data to same eBay listing ────────────
      // Use the same logic as handleRevise but with fresh scraped product
      // Mutate body to inject the fresh product and call handleRevise internally
      const revisedBody = {
        ...body,
        action: 'revise',
        // Force fresh scraped data — no fallback to old cached data
        fallbackImages:          freshProduct.images        || [],
        fallbackTitle:           freshProduct.title         || '',
        fallbackPrice:           freshProduct.price         || 0,
        fallbackInStock:         freshProduct.inStock !== false,
        fallbackComboAsin:       freshProduct.comboAsin     || null,
        fallbackComboInStock:    freshProduct.comboInStock  || null,
        fallbackComboPrices:     freshProduct.comboPrices   || null,
        fallbackVariations:      freshProduct.variations    || null,
        fallbackVariationImages: freshProduct.variationImages || null,
        fallbackPrimaryDimName:  freshProduct._primaryDimName || null,
        fallbackSecondaryDimName: freshProduct._secondaryDimName || null,
      };
      // Delegate to handleRevise with the fresh product injected
      return handleRevise({ body: revisedBody, res, getCategories, aiEnrich, sanitizeTitle, sleep, getEbayUrls });
    }

    // ── RECOVER SKUS — maps all eBay listing IDs → real SKUs ────────────────
    if (action === 'recoverSkus') {
      const { access_token } = body;
      if (!access_token) return res.status(400).json({ error: 'Missing access_token' });
      const EBAY_API = getEbayUrls(false).EBAY_API;
      const auth = { Authorization: `Bearer ${access_token}`, 'Accept-Language': 'en-US' };
      const skuMap = {}; // listingId → sku
      let offset = 0, hasMore = true;
      while (hasMore && offset < 2000) {
        const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=100&offset=${offset}`, { headers: auth });
        const d = await r.json();
        const offers = d.offers || [];
        for (const o of offers) {
          const lid = o.listing?.listingId;
          if (lid && o.sku) skuMap[lid] = o.sku;
        }
        hasMore = offers.length === 100;
        offset += 100;
        if (offers.length) await new Promise(r => setTimeout(r, 100));
      }
      return res.json({ skuMap, total: Object.keys(skuMap).length });
    }

    return res.status(400).json({ error: `Unknown action: ${action}` });

  } catch (err) {
    console.error('[Error]', err.message);
    return res.status(500).json({ error: err.message });
  }
};
