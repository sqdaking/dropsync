// DropSync — eBay Backend v4 (Vercel Serverless)
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
        'https://api.ebay.com/oauth/api_scope/sell.account',
        'https://api.ebay.com/oauth/api_scope/sell.account.readonly',
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
    // Parent snippet — checks what's on the parent ASIN page
    if (action === 'parent') {
      let url = req.query.url || body.url;
      if (!url) return res.json({ error: 'No URL' });
      const asinM = url.match(/\/dp\/([A-Z0-9]{10})/);
      if (!asinM) return res.json({ error: 'No ASIN found' });
      // First fetch landing to get parentAsin
      const ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36';
      const r1 = await fetch(`https://www.amazon.com/dp/${asinM[1]}`, { headers: { 'User-Agent': ua, 'Accept': 'text/html', 'Accept-Language': 'en-US,en;q=0.9' } });
      const h1 = await r1.text();
      const pM = h1.match(/parentAsin[=:&"']+([A-Z0-9]{10})/);
      const parentAsin = pM ? pM[1] : null;
      if (!parentAsin) return res.json({ error: 'No parentAsin found', landingSize: h1.length });
      // Fetch parent page
      const r2 = await fetch(`https://www.amazon.com/dp/${parentAsin}`, { headers: { 'User-Agent': ua, 'Accept': 'text/html', 'Accept-Language': 'en-US,en;q=0.9' } });
      const h2 = await r2.text();
      const snippets = {};
      for (const key of ['updateDivLists','asinVariationValues','priceToAsinList','colorImages','variationValues','unavailableAsinSet']) {
        const idx = h2.indexOf(key);
        snippets[key] = idx >= 0 ? h2.slice(idx, idx + 2000) : 'NOT FOUND';
      }
      return res.json({ parentAsin, landingSize: h1.length, parentSize: h2.length, snippets });
    }

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

          const fetchPage = async (u) => {
            try {
              const r = await fetch(u, { headers: { 'User-Agent': ua, 'Accept': 'text/html', 'Accept-Language': 'en-US,en;q=0.9' } });
              const h = await r.text();
              return (h.includes('robot check') || h.includes('Type the characters')) ? '' : h;
            } catch { return ''; }
          };

          // ── Title / Brand / Base price from landing page
          const titleM = html.match(/id="productTitle"[^>]*>\s*([\s\S]*?)\s*<\/span>/);
          if (titleM) product.title = titleM[1].replace(/<[^>]+>/g,'').trim().replace(/\s+/g,' ');
          const brandM = html.match(/id="bylineInfo"[^>]*>([\s\S]*?)<\/(?:a|span)>/);
          if (brandM) product.brand = brandM[1].replace(/<[^>]+>/g,'').replace(/Visit the|Store/g,'').trim();
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

          // ── Fetch parent page (has full variation data)
          const landingAsin = url.match(/\/dp\/([A-Z0-9]{10})/)?.[1] || '';
          const parentAsinM = html.match(/parentAsin[=:&"']+([A-Z0-9]{10})/);
          const parentAsin = parentAsinM?.[1] || null;
          let workHtml = html;
          if (parentAsin && parentAsin !== landingAsin) {
            const ph = await fetchPage(`https://www.amazon.com/dp/${parentAsin}`);
            if (ph.length > 50000) workHtml = ph;
          }

          // ── Extract variationValues + dimension order
          // variationValues: {"size_name":["val1","val2"], "color_name":["RED","BLUE"]}
          const variationValues = {};
          const dimOrder = []; // order from "dimensions" array is most reliable
          const dimOrderM = workHtml.match(/"dimensions"\s*:\s*(\[[^\]]+\])/);
          if (dimOrderM) {
            try { JSON.parse(dimOrderM[1]).forEach(d => dimOrder.push(d)); } catch {}
          }
          const vvM = workHtml.match(/"variationValues"\s*:\s*(\{[\s\S]*?\})\s*,\s*"\w/);
          if (vvM) {
            try {
              const vv = JSON.parse(vvM[1]);
              // Use dimOrder if available, else Object.keys
              const keys = dimOrder.length ? dimOrder : Object.keys(vv);
              for (const k of keys) if (Array.isArray(vv[k]) && vv[k].length) variationValues[k] = vv[k];
            } catch {}
          }
          if (!dimOrder.length) Object.keys(variationValues).forEach(k => dimOrder.push(k));

          // ── Extract dimensionValuesDisplayData: ASIN -> [dim0val, dim1val, ...]
          // Use brace-walking to get the full block (regex stops at first "}")
          const asinDimVals = {};
          function extractJsonBlock(h, key) {
            const ki = h.indexOf(`"${key}"`);
            if (ki < 0) return null;
            const bi = h.indexOf('{', ki);
            if (bi < 0) return null;
            let depth = 0, inStr = false, esc = false;
            for (let i = bi; i < Math.min(h.length, bi + 200000); i++) {
              const c = h[i];
              if (esc) { esc = false; continue; }
              if (c === '\\') { esc = true; continue; }
              if (c === '"') { inStr = !inStr; continue; }
              if (inStr) continue;
              if (c === '{') depth++;
              else if (c === '}') { depth--; if (depth === 0) return h.slice(bi, i + 1); }
            }
            return null;
          }
          const dvddRaw = extractJsonBlock(workHtml, 'dimensionValuesDisplayData') ||
                          extractJsonBlock(html, 'dimensionValuesDisplayData');
          if (dvddRaw) {
            try {
              const dvdd = JSON.parse(dvddRaw);
              for (const [asin, vals] of Object.entries(dvdd)) {
                if (Array.isArray(vals)) asinDimVals[asin] = vals;
              }
            } catch {}
          }

          // ── Extract per-ASIN prices by fetching individual variant pages
          // We fetch up to 4 representative ASINs to get real prices
          const asinPrice = {}; // asin -> price string
          const asinStock = {}; // asin -> bool
          const asinList = Object.keys(asinDimVals);

          // Try to get prices from HTML first (current selection + any inline data)
          // Pattern: "B0XXXXXX":"19.99" price map
          const inlinePrices = [...workHtml.matchAll(/"([A-Z0-9]{10})"\s*:\s*\{\s*"price"\s*:\s*"([\d.]+)"/g)];
          inlinePrices.forEach(m => { asinPrice[m[1]] = m[2]; });

          // priceToAsinList fallback: {"19.99":["B0XX","B0YY"]}
          const p2aM = workHtml.match(/"priceToAsinList"\s*:\s*(\{[\s\S]*?\})\s*,\s*"/);
          if (p2aM) {
            try {
              const p2a = JSON.parse(p2aM[1]);
              for (const [price, asins] of Object.entries(p2a))
                if (Array.isArray(asins)) asins.forEach(a => { if (!asinPrice[a]) asinPrice[a] = price; });
            } catch {}
          }

          // unavailableAsinSet — explicit out of stock list
          const unavailM = workHtml.match(/"unavailableAsinSet"\s*:\s*(\[[^\]]*\])/);
          if (unavailM) { try { JSON.parse(unavailM[1]).forEach(a => { asinStock[a] = false; }); } catch {} }
          // inStockAsinSet — explicit in stock list  
          const inStockM = workHtml.match(/"inStockAsinSet"\s*:\s*(\[[^\]]*\])/);
          if (inStockM) { try { JSON.parse(inStockM[1]).forEach(a => { asinStock[a] = true; }); } catch {} }
          // If neither set exists, leave asinStock empty — defaults to in stock below

          // Declare image maps BEFORE fetch so the async callbacks can write into them
          const colorImgMap = {};
          const allImages = [];

          // Fetch individual ASIN pages for prices + per-color images
          // Pick one representative ASIN per unique color to get its image
          const asinToColor = {}; // asin -> color value
          for (const [asin, dimVals] of Object.entries(asinDimVals)) {
            const ci = dimOrder.indexOf('color_name');
            if (ci >= 0 && dimVals[ci]) asinToColor[asin] = dimVals[ci];
          }
          // One ASIN per color (first found)
          const colorToAsin = {};
          for (const [asin, color] of Object.entries(asinToColor)) {
            if (!colorToAsin[color]) colorToAsin[color] = asin;
          }
          // Merge: need prices for all ASINs we don't have yet + one per color for images
          const needPrice = asinList.filter(a => !asinPrice[a]);
          const needImage = Object.values(colorToAsin);
          const toFetch = [...new Set([...needPrice, ...needImage])].slice(0, 8);

          await Promise.all(toFetch.map(async asin => {
            try {
              const h = await fetchPage(`https://www.amazon.com/dp/${asin}`);
              if (!h) return;
              // Price
              for (const pat of [
                /class="a-price-whole"[^>]*>\s*(\d[\d,]*)<\/span><span[^>]*class="a-price-fraction"[^>]*>\s*(\d+)/,
                /"priceAmount"\s*:\s*([\d.]+)/,
                /class="a-offscreen"[^>]*>\$([\d,]+\.\d{2})/,
              ]) {
                const m = h.match(pat);
                if (m) { asinPrice[asin] = m[2] ? `${m[1].replace(/,/g,'')}.${m[2]}` : m[1].replace(/,/g,''); break; }
              }
              // Stock — default to IN STOCK unless we explicitly see unavailable
              if (h.includes('Currently unavailable') || h.includes('currently unavailable') || h.includes('unavailable') && h.includes('this item') || h.includes('Item under review')) {
                asinStock[asin] = false;
              } else {
                // Any of these signals = in stock
                asinStock[asin] = true;
              }
              // Grab images from this ASIN's page
              // The 'initial' block always contains this ASIN's color-specific images
              const hiResOnPage = [...h.matchAll(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/g)]
                .map(m => m[1]);
              if (hiResOnPage.length) {
                const color = asinToColor[asin];
                if (color && !colorImgMap[color]) colorImgMap[color] = hiResOnPage[0];
                hiResOnPage.forEach(s => { if (!allImages.includes(s)) allImages.push(s); });
              }
            } catch {}
          }));

          // ── Seed allImages + extractColorImages helper
          function extractColorImages(h) {
            const map = {}, all = [];
            const ki = h.indexOf("'colorImages'");
            if (ki < 0) return { map, all };
            const bi = h.indexOf('{', ki);
            if (bi < 0) return { map, all };
            // Walk to find end of outer object
            let depth=0, inStr=false, esc=false, blockEnd=-1;
            for (let i=bi; i<Math.min(h.length,bi+500000); i++) {
              const c=h[i];
              if(esc){esc=false;continue;}
              if(c==='\\'&&inStr){esc=true;continue;}
              if(c==="'"){inStr=!inStr;continue;}
              if(c==='"'){inStr=!inStr;continue;}
              if(inStr)continue;
              if(c==='{')depth++;
              else if(c==='}'){depth--;if(depth===0){blockEnd=i;break;}}
            }
            if(blockEnd<0) return { map, all };
            const block = h.slice(bi, blockEnd+1);
            // Extract each color key and its image array
            // Keys look like: 'initial': [...] or 'BLACK': [...]
            // Use a manual scan since mixed quotes break JSON.parse
            const keyRe = /'([^']+)'\s*:\s*\[/g;
            let km;
            while((km=keyRe.exec(block))!==null){
              const key=km[1];
              const arrStart=km.index+km[0].length-1;
              // Find matching ] 
              let ad=0,ai=false,ae=false,arrEnd=-1;
              for(let i=arrStart;i<Math.min(block.length,arrStart+100000);i++){
                const c=block[i];
                if(ae){ae=false;continue;}
                if(c==='\\'&&ai){ae=true;continue;}
                if(c==='"'){ai=!ai;continue;}
                if(ai)continue;
                if(c==='[')ad++;
                else if(c===']'){ad--;if(ad===0){arrEnd=i;break;}}
              }
              if(arrEnd<0)continue;
              const arrStr=block.slice(arrStart,arrEnd+1);
              try{
                const imgs=JSON.parse(arrStr);
                if(!Array.isArray(imgs)||!imgs.length)continue;
                // Get best URL: prefer hiRes, then large, then first key of main
                const getBest=i=>i.hiRes||i.large||(i.main?Object.keys(i.main)[0]:'')||'';
                if(key==='initial'){
                  imgs.forEach(i=>{const s=getBest(i);if(s&&!all.includes(s))all.push(s);});
                } else {
                  const best=imgs.find(i=>i.hiRes)||imgs.find(i=>i.large)||imgs[0];
                  const s=best?getBest(best):'';
                  if(s)map[key]=s;
                }
              }catch{}
            }
            return { map, all };
          }

          // Seed allImages from landing page hiRes (initial color images)
          for (const m of html.matchAll(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/g))
            if (!allImages.includes(m[1])) allImages.push(m[1]);
          product.images = [...new Set(allImages)].slice(0, 12);
          // variationImages set after ASIN fetches below (colorImgMap populated there)

          // Landing ASIN image — already have its HTML, map to its color
          const landingColor = asinToColor[landingAsin];
          if (landingColor && !colorImgMap[landingColor]) {
            const landingImg = html.match(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/);
            if (landingImg) colorImgMap[landingColor] = landingImg[1];
          }
          // Also seed allImages from landing page if thin
          if (allImages.length < 5) {
            for (const m of html.matchAll(/"hiRes"\s*:\s*"(https:\/\/m\.media-amazon\.com\/images\/I\/[^"]+\.jpg)"/g))
              if (!allImages.includes(m[1])) allImages.push(m[1]);
          }

          // Finalize images + variationImages after per-ASIN fetches
          product.images = [...new Set(allImages)].slice(0, 12);
          if (Object.keys(colorImgMap).length) product.variationImages['Color'] = colorImgMap;

          // ── Build variation groups using dimensionValuesDisplayData
          const dimKeyToLabel = {
            'color_name':'Color','size_name':'Size','style_name':'Style',
            'material_type':'Material','pattern_name':'Pattern',
            'configuration_name':'Configuration','edition_name':'Edition',
            'item_package_quantity':'Package Quantity','scent_name':'Scent','flavor_name':'Flavor',
          };

          for (const [dimKey, values] of Object.entries(variationValues)) {
            if (!values.length) continue;
            const di = dimOrder.indexOf(dimKey);
            const label = dimKeyToLabel[dimKey] || dimKey.replace(/_name$/,'').replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase());

            const varValues = values.map(val => {
              const strVal = String(val);
              // Find all ASINs where this dimension position matches this value
              const matchAsins = Object.entries(asinDimVals)
                .filter(([, dimVals]) => di < dimVals.length && dimVals[di] === strVal)
                .map(([asin]) => asin);

              // Pick cheapest in-stock price
              let varPrice = product.price || '0';
              let inStock = true;
              let varAsin = matchAsins[0] || '';

              if (matchAsins.length) {
                const withPrices = matchAsins.filter(a => asinPrice[a]);
                const inStockOnes = withPrices.filter(a => asinStock[a] !== false);
                const use = inStockOnes.length ? inStockOnes : withPrices;
                if (use.length) {
                  const cheapest = use.reduce((a,b) => parseFloat(asinPrice[a]) <= parseFloat(asinPrice[b]) ? a : b);
                  varPrice = asinPrice[cheapest];
                  varAsin = cheapest;
                }
                // Stock: in stock if any matching ASIN is available
                const hasStockData = matchAsins.some(a => asinStock[a] !== undefined);
                inStock = hasStockData ? matchAsins.some(a => asinStock[a] !== false) : true;
              }

              return {
                value: strVal,
                price: varPrice,
                sourcePrice: varPrice,
                stock: inStock ? 10 : 0,
                inStock,
                asin: varAsin,
                image: dimKey === 'color_name' ? (colorImgMap[strVal] || allImages[0] || '') : '',
                enabled: inStock,
              };
            });
            product.variations.push({ name: label, values: varValues });
          }

          product.hasVariations = product.variations.length > 0;
          if (product.hasVariations) {
            const prices = product.variations.flatMap(vg => vg.values)
              .filter(v => v.inStock && parseFloat(v.price) > 0).map(v => parseFloat(v.price));
            if (prices.length) product.price = Math.min(...prices).toFixed(2);
          }

          // ── Description — use feature bullets div, not generic list items
          const featDiv = html.match(/id="feature-bullets"[^>]*>[\s\S]*?<ul[^>]*>([\s\S]*?)<\/ul>/);
          if (featDiv) {
            const bullets = [...featDiv[1].matchAll(/<span class="a-list-item">([\s\S]*?)<\/span>/g)]
              .map(m => m[1].replace(/<[^>]+>/g,'').replace(/&amp;/g,'&').replace(/&lt;/g,'<').replace(/&gt;/g,'>').trim())
              .filter(b => b.length > 10 && b.length < 600 && !b.includes('{') && !b.includes('\n\n'))
              .slice(0, 7);
            if (bullets.length) product.description = bullets.join('\n');
          }
          if (!product.description) {
            // Fallback: grab any list items that look like product features
            const bullets = [...html.matchAll(/<span class="a-list-item">([\s\S]*?)<\/span>/g)]
              .map(m => m[1].replace(/<[^>]+>/g,'').trim())
              .filter(b => b.length > 30 && b.length < 500 && !b.includes('{') && !b.includes('Kitchen') && !b.includes('Storage &'))
              .slice(0, 5);
            if (bullets.length) product.description = bullets.join('\n');
          }

          // ── Item specifics
          const specTable = html.match(/id="productDetails_techSpec[^"]*"[^>]*>([\s\S]*?)<\/table>/i) ||
                            html.match(/id="detailBullets_feature_div"[^>]*>([\s\S]*?)<\/div>/i);
          if (specTable) {
            for (const row of [...specTable[1].matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/g)].slice(0,12)) {
              const cells = [...row[1].matchAll(/<t[hd][^>]*>([\s\S]*?)<\/t[hd]>/g)]
                .map(m => m[1].replace(/<[^>]+>/g,'').replace(/[\u200f\u200e]/g,'').trim());
              if (cells.length >= 2 && cells[0] && cells[1] && cells[0].length < 60) product.aspects[cells[0]] = [cells[1]];
            }
          }

          product._debug = {
            landingAsin, parentAsin,
            asinCount: Object.keys(asinDimVals).length,
            pricesFetched: Object.keys(asinPrice).length,
            stockData: Object.keys(asinStock).length,
            images: allImages.length,
            colorMap: Object.keys(colorImgMap).length,
            dims: dimOrder,
            sampleAsinPrices: Object.fromEntries(Object.entries(asinPrice).slice(0, 5)),
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

      // SKU must be alphanumeric + hyphens only, max 50 chars
      const groupSku = `GRP${Date.now()}${Math.random().toString(36).slice(2,6).toUpperCase()}`;
      const varSkuBase = `ITM${Date.now()}${Math.random().toString(36).slice(2,6).toUpperCase()}`;
      const policies = {
        fulfillmentPolicyId: (body.fulfillmentPolicyId||'').split('?')[0].trim(),
        paymentPolicyId:     (body.paymentPolicyId||'').split('?')[0].trim(),
        returnPolicyId:      (body.returnPolicyId||'').split('?')[0].trim(),
      };
      const authHeader = { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json', 'Content-Language': 'en-US', 'Accept-Language': 'en-US' };

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
        const offerRes = await fetch(`${EBAY_API}/sell/inventory/v1/offer`, { method:'POST', headers:authHeader, body:JSON.stringify(buildOffer(groupSku, product, policies)) });
        const offerData = await offerRes.json();
        if (!offerRes.ok) return res.status(400).json({ error: 'Offer failed', details: offerData });
        const pubRes = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerData.offerId}/publish`, { method:'POST', headers:authHeader });
        const pubData = await pubRes.json();
        return res.json({ success:true, sku:groupSku, offerId:offerData.offerId, listingId:pubData.listingId });
      }

      // Clean up variations before building:
      // 1. Remove groups with only 1 value (eBay rejects single-value variation groups)
      // 2. Remove groups where all values have the same value as another group (duplicates)
      // 3. Move single-value groups to aspects instead
      const cleanVariations = product.variations.filter(vg => {
        const enabledVals = vg.values.filter(v => v.enabled !== false);
        return enabledVals.length >= 2; // eBay requires at least 2 values per variation dimension
      });
      // If we filtered out some groups, add their single values to aspects
      product.variations.filter(vg => {
        const enabledVals = vg.values.filter(v => v.enabled !== false);
        return enabledVals.length === 1;
      }).forEach(vg => {
        const v = vg.values.find(v => v.enabled !== false);
        if (v && !product.aspects[vg.name]) product.aspects[vg.name] = [v.value];
      });

      if (!cleanVariations.length) {
        // No valid variation groups — fall back to single listing
        const invRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(groupSku)}`, {
          method: 'PUT', headers: authHeader,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: parseInt(product.quantity)||10 } },
            condition: product.condition || 'NEW',
            product: { title: product.title.slice(0,80), description: product.description||product.title, imageUrls: (product.images||[]).slice(0,12), aspects: product.aspects||{} },
          }),
        });
        if (!invRes.ok) return res.status(400).json({ error: 'Inventory failed', details: await invRes.text() });
        const offerRes = await fetch(`${EBAY_API}/sell/inventory/v1/offer`, { method:'POST', headers:authHeader, body:JSON.stringify(buildOffer(groupSku, product, policies)) });
        const offerData = await offerRes.json();
        if (!offerRes.ok) return res.status(400).json({ error: 'Offer failed', details: offerData });
        const pubRes = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerData.offerId}/publish`, { method:'POST', headers:authHeader });
        const pubData = await pubRes.json();
        return res.json({ success:true, sku:groupSku, offerId:offerData.offerId, listingId:pubData.listingId });
      }

      // Use cleaned variations for combos
      product.variations = cleanVariations;

      // Multi-variation listing — eBay hard limit is 250 variations
      let combos = buildCombos(product.variations);
      if (combos.length > 250) {
        // Prioritize in-stock combos first, then trim to 250
        const inStock = combos.filter(c => c.every(v => (v.stock||0) > 0));
        const outStock = combos.filter(c => c.some(v => (v.stock||0) === 0));
        combos = [...inStock, ...outStock].slice(0, 250);
        console.log(`Trimmed combos from ${combos.length+outStock.length} to 250 (eBay limit)`);
      }
      const createdSkus = [];
      let firstSkuError = null;
      const usedSkus = new Set();

      for (const [comboIdx, combo] of combos.entries()) {
        // Skip only if EXPLICITLY disabled (not just out of stock — eBay wants quantity=0, not skipped)
        if (combo.every(v => v.enabled === false)) continue;
        // Build a clean SKU — sanitize each value segment
        const skuSegments = combo.map(v => {
          const clean = String(v.value||'').replace(/[^a-zA-Z0-9]/g,'').slice(0,6).toUpperCase();
          return clean || 'VAR';
        });
        let varSku = `${varSkuBase}${skuSegments.join('').slice(0,16)}`;
        // Guarantee uniqueness
        if (usedSkus.has(varSku)) varSku = `${varSkuBase}${String(comboIdx).padStart(4,'0')}`;
        usedSkus.add(varSku);
        const varPrice = combo.reduce((p,v) => v.price || p, product.price);
        const varStock = combo.reduce((s,v) => v.stock !== undefined ? v.stock : s, parseInt(product.quantity)||10);
        const varImages = getVarImages(combo, product.variationImages, product.images);
        const varAspects = { ...(product.aspects||{}) };
        combo.forEach(v => { varAspects[v.name] = [v.value]; });

        const invRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(varSku)}`, {
          method: 'PUT', headers: authHeader,
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: Math.max(0, parseInt(varStock)||0) } },
            condition: product.condition || 'NEW',
            product: { title: product.title.slice(0,80), description: product.description||product.title, imageUrls: varImages.slice(0,12), aspects: varAspects },
          }),
        });
        if (invRes.ok) {
          createdSkus.push(varSku);
          if (createdSkus.length === 1) console.log('first varSku ok:', varSku);
        } else {
          const errText = await invRes.text();
          console.error('varSku failed:', varSku, errText.slice(0,200));
          if (!firstSkuError) firstSkuError = { sku: varSku, status: invRes.status, body: errText.slice(0, 500) };
        }
      }

      if (!createdSkus.length) return res.status(400).json({ error: 'No variation SKUs created', details: firstSkuError });

      const groupRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`, {
        method: 'PUT', headers: authHeader,
        body: JSON.stringify({
          inventoryItemGroupKey: groupSku,
          title: product.title.slice(0,80),
          description: product.description || product.title,
          imageUrls: (product.images||[]).slice(0,12),
          aspects: product.aspects || {},
          variantSKUs: createdSkus,
          variesBy: {
            aspectsImageVariesBy: Object.keys(product.variationImages||{}),
            specifications: product.variations.map(vg => ({
              name: vg.name,
              values: vg.values.filter(v=>v.enabled!==false).map(v=>v.value)
            })),
          },
        }),
      });
      if (!groupRes.ok) {
        const groupErr = await groupRes.text();
        console.log('groupRes failed:', groupRes.status, groupErr.slice(0,300));
        return res.status(400).json({ error: 'Group failed', details: groupErr });
      }
      console.log('groupRes ok:', groupRes.status);

      // Get real merchant location key from eBay account
      let merchantLocationKey = 'default';
      try {
        const locRes = await fetch(`${EBAY_API}/sell/inventory/v1/location`, { headers: authHeader });
        const locData = await locRes.json();
        console.log('locations:', JSON.stringify(locData).slice(0,300));
        if (locData.locations?.length) {
          merchantLocationKey = locData.locations[0].merchantLocationKey;
        } else {
          const createRes = await fetch(`${EBAY_API}/sell/inventory/v1/location/MainWarehouse`, {
            method: 'POST', headers: authHeader,
            body: JSON.stringify({
              location: { address: { addressLine1: '1 Main St', city: 'San Jose', stateOrProvince: 'CA', postalCode: '95125', country: 'US' } },
              locationTypes: ['WAREHOUSE'], name: 'Main Warehouse', merchantLocationStatus: 'ENABLED'
            })
          });
          console.log('location create status:', createRes.status);
          merchantLocationKey = createRes.ok ? 'MainWarehouse' : 'default';
        }
      } catch(e) { console.log('location error:', e.message); }
      console.log('merchantLocationKey:', merchantLocationKey);

      // publishByInventoryItemGroup — no createOffer step needed for multi-variation
      const pubBody = {
        inventoryItemGroupKey: groupSku,
        marketplaceId: 'EBAY_US',
        merchantLocationKey,
        pricingSummary: { price: { value: String(parseFloat(product.price||0).toFixed(2)), currency: 'USD' } },
        listingDuration: 'GTC',
        categoryId: product.categoryId || '9355',
        format: 'FIXED_PRICE',
      };
      if (policies.fulfillmentPolicyId) pubBody.fulfillmentPolicyId = policies.fulfillmentPolicyId;
      if (policies.paymentPolicyId)     pubBody.paymentPolicyId     = policies.paymentPolicyId;
      if (policies.returnPolicyId)      pubBody.returnPolicyId      = policies.returnPolicyId;

      console.log('pubBody:', JSON.stringify(pubBody).slice(0,500));
      const pubRes = await fetch(`${EBAY_API}/sell/inventory/v1/offer/publishbyinventoryitemgroup`, {
        method: 'POST', headers: authHeader, body: JSON.stringify(pubBody)
      });
      const pubData = await pubRes.json();
      console.log('publishByGroup status:', pubRes.status, JSON.stringify(pubData).slice(0,500));
      if (!pubRes.ok) return res.status(400).json({ error: 'Publish failed', details: pubData });
      return res.json({ success:true, sku:groupSku, listingId:pubData.listingId, variationsCreated:createdSkus.length });
    }

    if (action === 'policies') {
      const token = req.query.access_token || body.access_token;
      if (!token) return res.status(400).json({ error: 'No token' });
      const h = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json', 'Accept-Language': 'en-US' };
      const [fp, pp, rp] = await Promise.all([
        fetch(`${EBAY_API}/sell/account/v1/fulfillment_policy?marketplace_id=EBAY_US`, { headers: h }).then(r => r.json()),
        fetch(`${EBAY_API}/sell/account/v1/payment_policy?marketplace_id=EBAY_US`, { headers: h }).then(r => r.json()),
        fetch(`${EBAY_API}/sell/account/v1/return_policy?marketplace_id=EBAY_US`, { headers: h }).then(r => r.json()),
      ]);
      console.log('fp:', JSON.stringify(fp).slice(0,300));
      console.log('pp:', JSON.stringify(pp).slice(0,300));
      console.log('rp:', JSON.stringify(rp).slice(0,300));
      return res.json({
        fulfillment: (fp.fulfillmentPolicies || []).map(p => ({ id: p.fulfillmentPolicyId, name: p.name })),
        payment:     (pp.paymentPolicies    || []).map(p => ({ id: p.paymentPolicyId,     name: p.name })),
        return:      (rp.returnPolicies     || []).map(p => ({ id: p.returnPolicyId,      name: p.name })),
        _raw: { fp, pp, rp }
      });
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

function buildOffer(sku, product, policies = {}, merchantLocationKey = 'default') {
  const p = { sku, marketplaceId:'EBAY_US', format:'FIXED_PRICE', listingDuration:'GTC', pricingSummary:{ price:{ value:String(parseFloat(product.price||0).toFixed(2)), currency:'USD' } }, categoryId:product.categoryId||'9355', merchantLocationKey };
  if (process.env.EBAY_FULFILLMENT_POLICY_ID) p.fulfillmentPolicyId = process.env.EBAY_FULFILLMENT_POLICY_ID;
  if (process.env.EBAY_PAYMENT_POLICY_ID)     p.paymentPolicyId     = process.env.EBAY_PAYMENT_POLICY_ID;
  if (process.env.EBAY_RETURN_POLICY_ID)      p.returnPolicyId      = process.env.EBAY_RETURN_POLICY_ID;
  // Source rules from frontend override env vars
  if (policies.fulfillmentPolicyId) p.fulfillmentPolicyId = policies.fulfillmentPolicyId;
  if (policies.paymentPolicyId)     p.paymentPolicyId     = policies.paymentPolicyId;
  if (policies.returnPolicyId)      p.returnPolicyId      = policies.returnPolicyId;
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
