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

          // ── Title / Brand
          const titleM = html.match(/id="productTitle"[^>]*>\s*([\s\S]*?)\s*<\/span>/);
          if (titleM) product.title = titleM[1].replace(/<[^>]+>/g,'').trim().replace(/\s+/g,' ');
          const brandM = html.match(/id="bylineInfo"[^>]*>([\s\S]*?)<\/(?:a|span)>/);
          if (brandM) product.brand = brandM[1].replace(/<[^>]+>/g,'').replace(/Visit the|Store/g,'').trim();

          // ── Base price
          for (const pat of [
            /class="a-price-whole"[^>]*>\s*(\d[\d,]*)<\/span><span[^>]*class="a-price-fraction"[^>]*>\s*(\d+)/,
            /"priceAmount"\s*:\s*([\d.]+)/,
            /id="priceblock_ourprice"[^>]*>\s*\$?([\d,]+\.?\d*)/,
            /"buyingPrice"\s*:\s*([\d.]+)/,
            /class="a-offscreen"[^>]*>\$([\d,]+\.\d{2})/,
          ]) {
            const m = html.match(pat);
            if (m) { product.price = m[2] ? `${m[1].replace(/,/g,'')}.${m[2]}` : m[1].replace(/,/g,''); break; }
          }

          // ── STEP 1: Find parentAsin from ajaxUrlParams
          const landingAsin = url.match(/\/dp\/([A-Z0-9]{10})/)?.[1] || '';
          const parentAsinM = html.match(/parentAsin[=:&"']+([A-Z0-9]{10})/);
          const parentAsin = parentAsinM ? parentAsinM[1] : null;

          // ── STEP 2: Fetch parent page (has full updateDivLists with all combos+prices)
          let workHtml = html;
          if (parentAsin && parentAsin !== landingAsin) {
            try {
              const pr = await fetch(`https://www.amazon.com/dp/${parentAsin}`, {
                headers: { 'User-Agent': ua, 'Accept': 'text/html', 'Accept-Language': 'en-US,en;q=0.9' }
              });
              const ph = await pr.text();
              if (ph.length > 50000 && !ph.includes('robot check')) workHtml = ph;
            } catch {}
          }

          // ── STEP 3: Extract variationValues {size_name:[...], color_name:[...]}
          const variationValues = {};
          const vvM = workHtml.match(/"variationValues"\s*:\s*(\{[\s\S]*?\})\s*,\s*"\w/) ||
                        html.match(/"variationValues"\s*:\s*(\{[\s\S]*?\})\s*,\s*"\w/);
          if (vvM) {
            try {
              const vv = JSON.parse(vvM[1]);
              for (const [k,v] of Object.entries(vv)) if (Array.isArray(v) && v.length) variationValues[k] = v;
            } catch {}
          }
          const dimOrder = Object.keys(variationValues);

          // ── STEP 4: Extract updateDivLists (combo_id -> {asin, price, inStock})
          const combos = {};
          function parseCombos(h) {
            const si = h.indexOf('"updateDivLists"');
            if (si < 0) return;
            // Walk braces to find the full block
            let depth=0, inStr=false, esc=false, start=-1;
            for (let i=si; i<Math.min(h.length, si+600000); i++) {
              const c=h[i];
              if (esc){esc=false;continue;}
              if (c==='\\'&&inStr){esc=true;continue;}
              if (c==='"'){inStr=!inStr;continue;}
              if (inStr) continue;
              if (c==='{'){if(depth===0)start=i;depth++;}
              else if(c==='}'){depth--;if(depth===0&&start>=0){
                const block=h.slice(start,i+1);
                // Each entry: "N_N":{"asin":"B0XX","price":"19.99",...}
                for (const [,id,body] of block.matchAll(/"(\d+(?:_\d+)+)"\s*:\s*\{([^{}]{0,600})\}/g)) {
                  const am=body.match(/"asin"\s*:\s*"([A-Z0-9]{10})"/);
                  const pm=body.match(/"(?:price|displayPrice)"\s*:\s*"?\$?([\d.]+)"?/);
                  const sm=body.match(/"(?:inStock|isAvailable|buyable)"\s*:\s*(true|false|1|0)/i);
                  if (am) combos[id]={asin:am[1], price:pm?pm[1]:null, inStock:sm?(sm[1]==='true'||sm[1]==='1'):true};
                }
                break;
              }}
            }
          }
          parseCombos(workHtml);
          if (!Object.keys(combos).length) parseCombos(html);

          // ── STEP 5: Extract images from colorImages (JS single-quote format)
          const colorImgMap = {};
          const allImages = [];
          function parseColorImages(h) {
            const m = h.match(/'colorImages'\s*:\s*\{([\s\S]*?)\}\s*,\s*'(?:colorToAsin|landing|selected|color)/);
            if (!m) return;
            // Extract each 'KEY': [...] entry
            for (const [,key,arr] of m[1].matchAll(/'([^']+)'\s*:\s*(\[[\s\S]*?\])(?=\s*,\s*'|\s*$)/g)) {
              try {
                const imgs = JSON.parse(arr);
                const best = imgs.find(i=>i.hiRes)||imgs.find(i=>i.large)||imgs[0];
                if (!best) continue;
                const src = best.hiRes||best.large||(best.main&&Object.keys(best.main)[0])||'';
                if (!src) continue;
                if (key==='initial') {
                  imgs.forEach(i=>{
                    const s=i.hiRes||i.large||(i.main&&Object.keys(i.main)[0]);
                    if(s&&!allImages.includes(s))allImages.push(s);
                  });
                } else {
                  colorImgMap[key]=src;
                }
              } catch {}
            }
          }
          parseColorImages(workHtml);
          parseColorImages(html);
          // Fallback
          if (!allImages.length) {
            [...html.matchAll(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon[^"]+)"/g)].forEach(m=>{if(!allImages.includes(m[1]))allImages.push(m[1]);});
          }
          product.images = [...new Set(allImages)].slice(0,12);
          if (Object.keys(colorImgMap).length) product.variationImages['Color'] = colorImgMap;

          // ── STEP 6: Build variation groups
          const dimKeyToLabel = {
            'color_name':'Color','size_name':'Size','style_name':'Style',
            'material_type':'Material','pattern_name':'Pattern',
            'configuration_name':'Configuration','edition_name':'Edition',
            'item_package_quantity':'Package Quantity','scent_name':'Scent','flavor_name':'Flavor',
          };
          const hasCombos = Object.keys(combos).length > 0;

          for (const [dimKey, values] of Object.entries(variationValues)) {
            if (!values.length) continue;
            const di = dimOrder.indexOf(dimKey);
            const label = dimKeyToLabel[dimKey]||dimKey.replace(/_name$/,'').replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase());
            const varValues = values.map((val,vi) => {
              let varPrice=product.price||'0', inStock=true, varAsin='';
              if (hasCombos) {
                const matching = Object.entries(combos).filter(([id])=>parseInt(id.split('_')[di])===vi);
                if (matching.length) {
                  const avail=matching.filter(([,c])=>c.inStock!==false&&c.price);
                  const use=avail.length?avail:matching.filter(([,c])=>c.price);
                  if (use.length) {
                    const cheapest=use.reduce((a,b)=>parseFloat(a[1].price)<=parseFloat(b[1].price)?a:b);
                    varPrice=cheapest[1].price; varAsin=cheapest[1].asin;
                  }
                  inStock=matching.some(([,c])=>c.inStock!==false);
                }
              }
              return {
                value:String(val), price:varPrice, sourcePrice:varPrice,
                stock:inStock?10:0, inStock, asin:varAsin,
                image:dimKey==='color_name'?(colorImgMap[String(val)]||allImages[0]||''):'',
                enabled:inStock,
              };
            });
            product.variations.push({ name:label, values:varValues });
          }
          product.hasVariations = product.variations.length > 0;
          if (product.hasVariations) {
            const prices=product.variations.flatMap(vg=>vg.values).filter(v=>v.inStock&&parseFloat(v.price)>0).map(v=>parseFloat(v.price));
            if (prices.length) product.price=Math.min(...prices).toFixed(2);
          }

          // ── Description
          const bullets=[...html.matchAll(/<span class="a-list-item">\s*([\s\S]*?)\s*<\/span>/g)]
            .map(m=>m[1].replace(/<[^>]+>/g,'').trim()).filter(b=>b.length>15&&b.length<500&&!b.includes('{')).slice(0,5);
          if (bullets.length) product.description=bullets.join('\n');

          // ── Item specifics
          const specTable=html.match(/id="productDetails_techSpec[^"]*"[^>]*>([\s\S]*?)<\/table>/i)||
                          html.match(/id="detailBullets_feature_div"[^>]*>([\s\S]*?)<\/div>/i);
          if (specTable) {
            for (const row of [...specTable[1].matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/g)].slice(0,12)) {
              const cells=[...row[1].matchAll(/<t[hd][^>]*>([\s\S]*?)<\/t[hd]>/g)].map(m=>m[1].replace(/<[^>]+>/g,'').replace(/[\u200f\u200e]/g,'').trim());
              if (cells.length>=2&&cells[0]&&cells[1]&&cells[0].length<60) product.aspects[cells[0]]=[cells[1]];
            }
          }

          product._debug = { landingAsin, parentAsin, combos:Object.keys(combos).length, images:allImages.length, colorMap:Object.keys(colorImgMap).length, dims:dimOrder, hasCombos };
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
