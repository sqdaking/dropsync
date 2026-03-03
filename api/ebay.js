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
      if (!code) return res.status(400).json({ error: 'No code' });
      const r = await fetch(`${EBAY_API}/identity/v1/oauth2/token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', Authorization: basicAuth },
        body: new URLSearchParams({ grant_type: 'authorization_code', code, redirect_uri: RUNAME }),
      });
      const d = await r.json();
      return res.redirect(`${FRONTEND_URL}?access_token=${d.access_token}&refresh_token=${d.refresh_token}&expires_in=${d.expires_in}`);
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
    if (action === 'scrape') {
      const url = body.url || req.query.url;
      if (!url) return res.status(400).json({ error: 'No URL' });

      const product = {
        url, source: '', title: '', price: '', images: [],
        description: '', brand: '', aspects: {},
        variations: [], variationImages: {}, hasVariations: false,
      };

      try {
        const htmlRes = await fetch(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache',
          },
        });
        const html = await htmlRes.text();

        // ── AMAZON ──────────────────────────────────────────────────────
        if (url.includes('amazon.com')) {
          product.source = 'amazon';

          // ── Title
          const titleM = html.match(/id="productTitle"[^>]*>\s*([\s\S]*?)\s*<\/span>/);
          if (titleM) product.title = titleM[1].replace(/<[^>]+>/g,'').trim().replace(/\s+/g,' ');

          // ── Brand
          const brandM = html.match(/id="bylineInfo"[^>]*>([\s\S]*?)<\/(?:a|span)>/);
          if (brandM) product.brand = brandM[1].replace(/<[^>]+>/g,'').replace(/Visit the|Store/g,'').trim();

          // ── Base price (multiple fallbacks)
          const priceSelectors = [
            /class="a-price-whole"[^>]*>\s*(\d[\d,]*)\s*<\/span>\s*<span[^>]*class="a-price-fraction"[^>]*>\s*(\d+)/,
            /"priceAmount"\s*:\s*([\d.]+)/,
            /id="priceblock_ourprice"[^>]*>\s*\$?([\d,]+\.?\d*)/,
            /id="priceblock_dealprice"[^>]*>\s*\$?([\d,]+\.?\d*)/,
            /"buyingPrice"\s*:\s*([\d.]+)/,
            /class="a-offscreen"[^>]*>\$([\d,]+\.?\d*)/,
            /\$\s*([\d,]+\.\d{2})/,
          ];
          for (const pat of priceSelectors) {
            const m = html.match(pat);
            if (m) {
              product.price = m[2] ? `${m[1].replace(/,/g,'')}.${m[2]}` : m[1].replace(/,/g,'');
              break;
            }
          }

          // ══════════════════════════════════════════════════════
          // AMAZON VARIATION EXTRACTION
          // Strategy: parse the embedded JSON data blocks that
          // Amazon uses to power the variation selector (twister)
          // ══════════════════════════════════════════════════════

          // 1) Extract color→image mapping from colorImages block
          const colorImgMap = {};
          const colorImagesMatch = html.match(/'colorImages'\s*:\s*\{([\s\S]*?)\},\s*'colorToAsin'/);
          if (colorImagesMatch) {
            try {
              const parsed = JSON.parse(`{${colorImagesMatch[1]}}`);
              for (const [colorKey, imgs] of Object.entries(parsed)) {
                if (!Array.isArray(imgs) || colorKey === 'initial') continue;
                const best = imgs.find(i => i.hiRes) || imgs.find(i => i.large) || imgs[0];
                if (best) colorImgMap[colorKey] = best.hiRes || best.large || '';
              }
            } catch {}
          }

          // 2) Extract all product images (from initial colorImages entry)
          const allProductImages = [];
          const initImagesMatch = html.match(/'colorImages'\s*:\s*\{[^}]*"initial"\s*:\s*(\[[\s\S]*?\])\s*[,}]/);
          if (initImagesMatch) {
            try {
              const imgs = JSON.parse(initImagesMatch[1]);
              imgs.forEach(i => {
                const src = i.hiRes || i.large;
                if (src && !allProductImages.includes(src)) allProductImages.push(src);
              });
            } catch {}
          }
          // Fallback image extraction
          if (!allProductImages.length) {
            const hiRes = [...html.matchAll(/"hiRes"\s*:\s*"(https[^"]+)"/g)].map(m => m[1]);
            const large = [...html.matchAll(/"large"\s*:\s*"(https[^"]+)"/g)].map(m => m[1]);
            allProductImages.push(...[...new Set([...hiRes, ...large])]);
          }
          product.images = allProductImages.slice(0, 12);

          // 3) Extract dimension names (e.g. "Size", "Color", "Style")
          const dimensionNames = [];
          const dimNameMatches = [...html.matchAll(/"variationDisplayLabel"\s*:\s*"([^"]+)"/g)];
          dimNameMatches.forEach(m => { if (!dimensionNames.includes(m[1])) dimensionNames.push(m[1]); });

          // Also try: "dimensionValuesDisplayData"
          const altDimMatch = html.match(/"dimensionValuesDisplayData"\s*:\s*(\{[\s\S]*?\})\s*,\s*"[a-z]/);
          
          // 4) Extract variation values per dimension
          // Try the twister data block first (most reliable)
          const variationData = {};
          
          // Method A: variationValues block
          const varValuesMatch = html.match(/"variationValues"\s*:\s*(\{[\s\S]*?\})\s*,\s*"[a-zA-Z]/);
          if (varValuesMatch) {
            try {
              const vv = JSON.parse(varValuesMatch[1]);
              for (const [dimName, values] of Object.entries(vv)) {
                if (Array.isArray(values)) variationData[dimName] = values;
              }
            } catch {}
          }

          // Method B: dimensionValuesDisplayData
          const dvddMatch = html.match(/"dimensionValuesDisplayData"\s*:\s*(\{[\s\S]*?\})\s*,\s*"[a-z]/);
          if (dvddMatch && !Object.keys(variationData).length) {
            try {
              const dvdd = JSON.parse(dvddMatch[1]);
              for (const [key, val] of Object.entries(dvdd)) {
                if (Array.isArray(val)) variationData[key] = val;
              }
            } catch {}
          }

          // Method C: scan script blocks for color_name and size_name arrays
          if (!Object.keys(variationData).length) {
            const scripts = [...html.matchAll(/<script[^>]*type="text\/javascript"[^>]*>([\s\S]*?)<\/script>/g)].map(m => m[1]);
            for (const script of scripts) {
              if (!script.includes('color_name') && !script.includes('size_name') && !script.includes('variation')) continue;
              // color
              const cM = script.match(/"color_name"\s*:\s*(\[[^\]]+\])/);
              if (cM) { try { const c = JSON.parse(cM[1]); if (c.length) variationData['color_name'] = c; } catch {} }
              // size
              const sM = script.match(/"size_name"\s*:\s*(\[[^\]]+\])/);
              if (sM) { try { const s = JSON.parse(sM[1]); if (s.length) variationData['size_name'] = s; } catch {} }
              // style
              const stM = script.match(/"style_name"\s*:\s*(\[[^\]]+\])/);
              if (stM) { try { const st = JSON.parse(stM[1]); if (st.length) variationData['style_name'] = st; } catch {} }
              if (Object.keys(variationData).length) break;
            }
          }

          // Method D: parse select dropdowns in HTML
          const sizeDropdown = html.match(/name="dropdown_selected_size_name"[\s\S]*?<select[^>]*>([\s\S]*?)<\/select>/i);
          if (sizeDropdown && !variationData['size_name']) {
            const opts = [...sizeDropdown[1].matchAll(/<option[^>]+value="([^"]+)"[^>]*>([^<]+)<\/option>/g)]
              .filter(m => m[1] && m[1] !== '-1' && !m[1].includes('Select'))
              .map(m => m[2].trim());
            if (opts.length) variationData['size_name'] = opts;
          }

          // 5) Extract per-ASIN prices from asinVariationValues or unifiedPrice
          const asinPriceMap = {};
          const asinDimMap = {}; // asin -> { color_name: 'Red', size_name: 'L' }

          // Try to find asinToDimension mapping
          const asinToDimMatch = html.match(/"asinToDimension"\s*:\s*(\{[\s\S]*?\})\s*,\s*"[a-zA-Z]/);
          if (asinToDimMatch) {
            try { Object.assign(asinDimMap, JSON.parse(asinToDimMatch[1])); } catch {}
          }

          // Try merchantCustomerPreference or offerListings for per-ASIN prices
          const offerMatch = html.match(/"offerListings"\s*:\s*(\[[\s\S]*?\])\s*,\s*"[a-zA-Z]/);
          if (offerMatch) {
            try {
              const offers = JSON.parse(offerMatch[1]);
              offers.forEach(o => { if (o.asin && o.price?.amount) asinPriceMap[o.asin] = String(o.price.amount); });
            } catch {}
          }

          // 6) Build variation groups
          const dimKeyToLabel = {
            'color_name': 'Color', 'size_name': 'Size', 'style_name': 'Style',
            'material_type': 'Material', 'pattern_name': 'Pattern',
            'configuration_name': 'Configuration', 'edition_name': 'Edition',
          };

          for (const [dimKey, values] of Object.entries(variationData)) {
            if (!Array.isArray(values) || !values.length) continue;
            const label = dimKeyToLabel[dimKey] || dimNameMatches.find(m => m[1].toLowerCase().includes(dimKey.split('_')[0]))?.[ 1] || dimKey.replace(/_name$/,'').replace(/_/g,' ').replace(/\b\w/g,c=>c.toUpperCase());
            
            const varValues = values.map(val => {
              const strVal = String(val);
              const img = dimKey === 'color_name' ? (colorImgMap[strVal] || '') : '';
              // Find price for this variant if available
              let varPrice = product.price;
              for (const [asin, dims] of Object.entries(asinDimMap)) {
                if (dims[dimKey] === strVal && asinPriceMap[asin]) { varPrice = asinPriceMap[asin]; break; }
              }
              return { value: strVal, price: varPrice, stock: 10, image: img, enabled: true };
            });

            product.variations.push({ name: label, values: varValues });

            // Build variation image map for Color
            if (dimKey === 'color_name' && Object.keys(colorImgMap).length) {
              product.variationImages['Color'] = colorImgMap;
            }
          }

          product.hasVariations = product.variations.length > 0;

          // 7) Description from bullet points
          const bullets = [...html.matchAll(/<span class="a-list-item">\s*([\s\S]*?)\s*<\/span>/g)]
            .map(m => m[1].replace(/<[^>]+>/g,'').trim())
            .filter(b => b.length > 15 && b.length < 500 && !b.includes('{'))
            .slice(0, 5);
          if (bullets.length) product.description = bullets.join('\n');
          if (!product.description) {
            const featureDiv = html.match(/id="feature-bullets"[^>]*>([\s\S]*?)<\/div>/);
            if (featureDiv) product.description = featureDiv[1].replace(/<[^>]+>/g,'').replace(/\s+/g,' ').trim().slice(0,1000);
          }

          // 8) Item specifics from product details table
          const specTable = html.match(/id="productDetails_techSpec[^"]*"[^>]*>([\s\S]*?)<\/table>/i) ||
                            html.match(/id="detailBullets_feature_div"[^>]*>([\s\S]*?)<\/div>/i);
          if (specTable) {
            const rows = [...specTable[1].matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/g)];
            for (const row of rows.slice(0, 12)) {
              const cells = [...row[1].matchAll(/<t[hd][^>]*>([\s\S]*?)<\/t[hd]>/g)].map(m => m[1].replace(/<[^>]+>/g,'').replace(/\u200f|\u200e/g,'').trim());
              if (cells.length >= 2 && cells[0] && cells[1] && cells[0].length < 60) {
                product.aspects[cells[0]] = [cells[1]];
              }
            }
          }
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
