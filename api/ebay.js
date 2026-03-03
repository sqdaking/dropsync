// DropSync — eBay Backend API with Variation Support
// Deploy to Vercel. Set env vars in Vercel dashboard.

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

  const basicAuth = 'Basic ' + Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64');
  const { action } = req.query;

  let body = {};
  if (req.method === 'POST') {
    try { body = typeof req.body === 'string' ? JSON.parse(req.body) : req.body || {}; } catch {}
  }

  try {

    // ── OAuth start ───────────────────────────────────────────────────────────
    if (action === 'auth') {
      const scopes = [
        'https://api.ebay.com/oauth/api_scope',
        'https://api.ebay.com/oauth/api_scope/sell.inventory',
        'https://api.ebay.com/oauth/api_scope/sell.inventory.readonly',
        'https://api.ebay.com/oauth/api_scope/sell.fulfillment',
        'https://api.ebay.com/oauth/api_scope/sell.fulfillment.readonly',
      ].join(' ');
      const url = `${EBAY_AUTH}/oauth2/authorize?client_id=${CLIENT_ID}&redirect_uri=${encodeURIComponent(RUNAME)}&response_type=code&scope=${encodeURIComponent(scopes)}`;
      return res.redirect(url);
    }

    // ── OAuth callback ────────────────────────────────────────────────────────
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

    // ── Refresh token ─────────────────────────────────────────────────────────
    if (action === 'refresh') {
      const refresh_token = body.refresh_token || req.query.refresh_token;
      const r = await fetch(`${EBAY_API}/identity/v1/oauth2/token`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded', Authorization: basicAuth },
        body: new URLSearchParams({ grant_type: 'refresh_token', refresh_token }),
      });
      return res.json(await r.json());
    }

    // ── Scrape product with full variation extraction ─────────────────────────
    if (action === 'scrape') {
      const url = body.url || req.query.url;
      if (!url) return res.status(400).json({ error: 'No URL' });

      const product = {
        url, title: '', price: '', images: [], description: '',
        source: '', brand: '', aspects: {},
        variations: [],   // [{ name:'Size', values:[{value:'S',price:'',sku:'',image:'',stock:1}] }]
        variationImages: {}, // { 'Color': { 'Red': 'https://...' } }
        hasVariations: false,
      };

      try {
        const htmlRes = await fetch(url, {
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'no-cache',
          },
        });
        const html = await htmlRes.text();

        // ── AMAZON ───────────────────────────────────────────────────────────
        if (url.includes('amazon.com')) {
          product.source = 'amazon';

          // Title
          const titleM = html.match(/id="productTitle"[^>]*>([\s\S]*?)<\/span>/);
          if (titleM) product.title = titleM[1].trim().replace(/\s+/g, ' ');

          // Brand
          const brandM = html.match(/id="bylineInfo"[^>]*>([\s\S]*?)<\/a>/);
          if (brandM) product.brand = brandM[1].replace(/<[^>]+>/g, '').replace('Visit the','').replace('Store','').trim();

          // Price — try multiple selectors
          const pricePatterns = [
            /"priceAmount":(\d+\.?\d*)/,
            /class="a-price-whole"[^>]*>(\d+)/,
            /"lowPrice":"(\d+\.?\d*)"/,
            /\$(\d+\.\d{2})<\/span>/,
          ];
          for (const pat of pricePatterns) {
            const m = html.match(pat);
            if (m) { product.price = m[1]; break; }
          }

          // Images — extract from image block JSON
          const imgBlockM = html.match(/'colorImages'\s*:\s*(\{[\s\S]*?\})\s*,\s*'colorToAsin'/);
          if (imgBlockM) {
            try {
              const colorImgData = JSON.parse(imgBlockM[1]);
              const allImgs = [];
              const colorImageMap = {};
              for (const [color, imgs] of Object.entries(colorImgData)) {
                if (Array.isArray(imgs)) {
                  const large = imgs.map(i => i.hiRes || i.large).filter(Boolean);
                  if (color !== 'initial') colorImageMap[color] = large[0] || '';
                  allImgs.push(...large);
                }
              }
              product.images = [...new Set(allImgs)].slice(0, 12);
              if (Object.keys(colorImageMap).length > 0) product.variationImages['Color'] = colorImageMap;
            } catch {}
          }
          // Fallback images
          if (!product.images.length) {
            const hiResImgs = [...html.matchAll(/"hiRes":"([^"]+)"/g)].map(m => m[1]);
            const landImgs  = [...html.matchAll(/"large":"([^"]+)"/g)].map(m => m[1]);
            product.images = [...new Set([...hiResImgs, ...landImgs])].slice(0, 12);
          }

          // Variations — extract from twister JS data
          const variationJson = html.match(/var\s+dataToReturn\s*=\s*(\{[\s\S]*?\});\s*\n/);
          const dimensionNames = [...html.matchAll(/"variationDisplayLabel":"([^"]+)"/g)].map(m => m[1]);
          const dimensionValues = {};

          // Parse selected variations block
          const selectBtns = [...html.matchAll(/class="a-button-text"[^>]*>\s*([\w\s\-\/\.]+)\s*<\/span>/g)].map(m => m[1].trim());
          const swatchData = [...html.matchAll(/"swatchData"\s*:\s*\{([\s\S]*?)\}/g)];

          // Try JSON variation data embedded in page
          const variationStr = html.match(/P\.when\('A'\)\.register\("twister-js-init-dpx-data"[\s\S]*?variationValues\s*:\s*(\{[\s\S]*?\})\s*,\s*\n/);

          // More reliable: look for dimension_values in JSON
          const dimMatch = html.match(/"dimensionToAsinMap"\s*:\s*(\{[^}]+\})/);
          const asinDimMatch = html.match(/"dimensions"\s*:\s*(\[[^\]]+\])/);

          // Extract variations from the available_variations JSON
          const availVarMatch = html.match(/"twisterData"\s*:\s*(\{[\s\S]*?\})\s*}\s*\)\s*;/);
          
          // Fallback: parse dropdown/button variation sections
          const sizePat = html.match(/name="dropdown_selected_size_name"[\s\S]*?<select[^>]*>([\s\S]*?)<\/select>/i);
          if (sizePat) {
            const opts = [...sizePat[1].matchAll(/<option[^>]+value="([^"]+)"[^>]*>([^<]+)<\/option>/g)]
              .filter(m => m[1] && m[1] !== '-1')
              .map(m => ({ value: m[2].trim(), price: product.price, stock: 1 }));
            if (opts.length > 0) {
              product.variations.push({ name: 'Size', values: opts });
            }
          }

          // Color variations from swatch
          const colorSwatchPat = [...html.matchAll(/data-dp-url="([^"]+)"[\s\S]*?alt="([^"]+)"[\s\S]*?class="[^"]*swatch[^"]*"/g)];
          const colorNames = [...html.matchAll(/class="[^"]*twisterSlotDiv[^"]*"[\s\S]*?data-value="([^"]+)"/g)].map(m=>m[1]);

          // Try to extract structured variation data from page JSON
          const jsonVariations = [];
          const scriptBlocks = [...html.matchAll(/<script[^>]*>([\s\S]*?)<\/script>/gi)].map(m=>m[1]);
          for (const script of scriptBlocks) {
            if (!script.includes('variationValues') && !script.includes('twister')) continue;
            try {
              // Color options
              const colorOpts = [...script.matchAll(/"color_name":\s*\[([^\]]+)\]/gi)];
              if (colorOpts.length) {
                const colors = colorOpts[0][1].replace(/"/g,'').split(',').map(s=>s.trim()).filter(Boolean);
                if (colors.length > 0) jsonVariations.push({ name:'Color', values: colors.map(c=>({value:c,price:product.price,stock:1,image:product.variationImages?.Color?.[c]||''})) });
              }
              // Size options
              const sizeOpts = [...script.matchAll(/"size_name":\s*\[([^\]]+)\]/gi)];
              if (sizeOpts.length) {
                const sizes = sizeOpts[0][1].replace(/"/g,'').split(',').map(s=>s.trim()).filter(Boolean);
                if (sizes.length > 0) jsonVariations.push({ name:'Size', values: sizes.map(s=>({value:s,price:product.price,stock:1,image:''})) });
              }
            } catch {}
          }
          if (jsonVariations.length > 0 && product.variations.length === 0) {
            product.variations = jsonVariations;
          }

          product.hasVariations = product.variations.length > 0;

          // Description / bullet points
          const bullets = [...html.matchAll(/<span class="a-list-item">\s*([\s\S]*?)\s*<\/span>/g)]
            .map(m => m[1].replace(/<[^>]+>/g,'').trim())
            .filter(b => b.length > 10 && b.length < 300)
            .slice(0, 5);
          if (bullets.length) product.description = bullets.join('\n');

          // Aspects / item specifics
          const aspectRows = [...html.matchAll(/<tr[^>]*>[\s\S]*?<th[^>]*>([\s\S]*?)<\/th>[\s\S]*?<td[^>]*>([\s\S]*?)<\/td>[\s\S]*?<\/tr>/g)];
          for (const row of aspectRows.slice(0, 10)) {
            const k = row[1].replace(/<[^>]+>/g,'').trim();
            const v = row[2].replace(/<[^>]+>/g,'').trim();
            if (k && v && k.length < 50 && v.length < 200) product.aspects[k] = [v];
          }
        }

        // ── WALMART ──────────────────────────────────────────────────────────
        else if (url.includes('walmart.com')) {
          product.source = 'walmart';

          // Try to parse __NEXT_DATA__ JSON (most reliable)
          const nextDataM = html.match(/<script id="__NEXT_DATA__"[^>]*>([\s\S]*?)<\/script>/);
          if (nextDataM) {
            try {
              const nextData = JSON.parse(nextDataM[1]);
              const item = nextData?.props?.pageProps?.initialData?.data?.product;
              if (item) {
                product.title = item.name || '';
                product.price = item.priceInfo?.currentPrice?.price?.toString() || '';
                product.brand = item.brand || '';
                product.description = item.shortDescription?.replace(/<[^>]+>/g,'') || '';
                // Images
                if (item.imageInfo?.thumbnailUrl) product.images.push(item.imageInfo.thumbnailUrl);
                if (item.imageInfo?.allImages) product.images = item.imageInfo.allImages.map(i=>i.url||i).filter(Boolean).slice(0,12);
                // Variants
                if (item.variantCriteria) {
                  for (const crit of item.variantCriteria) {
                    const varGroup = {
                      name: crit.label || crit.id,
                      values: (crit.variantList || []).map(v => ({
                        value: v.name || v.id,
                        price: v.priceInfo?.currentPrice?.price?.toString() || product.price,
                        stock: v.availabilityStatus === 'IN_STOCK' ? 10 : 0,
                        image: v.images?.[0]?.url || '',
                      }))
                    };
                    if (varGroup.values.length > 0) product.variations.push(varGroup);
                    if (crit.label === 'Color' || crit.id === 'actual_color') {
                      const imgMap = {};
                      for (const v of varGroup.values) if (v.image) imgMap[v.value] = v.image;
                      if (Object.keys(imgMap).length) product.variationImages['Color'] = imgMap;
                    }
                  }
                  product.hasVariations = product.variations.length > 0;
                }
                // Aspects
                if (item.specifications) {
                  for (const spec of (item.specifications || []).slice(0,10)) {
                    if (spec.name && spec.value) product.aspects[spec.name] = [spec.value];
                  }
                }
              }
            } catch(e) {}
          }

          // Fallbacks
          if (!product.title) {
            const m = html.match(/"name":"([^"]{10,200})"/);
            if (m) product.title = m[1];
          }
          if (!product.price) {
            const m = html.match(/"price":(\d+\.?\d*)/);
            if (m) product.price = m[1];
          }
          if (!product.images.length) {
            const imgs = [...html.matchAll(/"url":"(https:\/\/i5\.walmartimages\.com[^"]+)"/g)].map(m=>m[1]);
            product.images = [...new Set(imgs)].slice(0,12);
          }
        }

        // ── ALIEXPRESS ───────────────────────────────────────────────────────
        else if (url.includes('aliexpress.com')) {
          product.source = 'aliexpress';

          // Try window.runParams JSON
          const runParamsM = html.match(/window\.runParams\s*=\s*(\{[\s\S]*?\});\s*\n/);
          if (runParamsM) {
            try {
              const rp = JSON.parse(runParamsM[1]);
              const data = rp?.data?.productInfoComponent || rp?.data;
              if (data) {
                product.title = data.subject || data.title || '';
                product.price = data.priceComponent?.discountPrice?.formatedAmount?.replace(/[^0-9.]/g,'') ||
                                data.priceComponent?.originalPrice?.formatedAmount?.replace(/[^0-9.]/g,'') || '';
                product.description = data.description || '';
                if (data.imagePathList) product.images = data.imagePathList.map(i=>`https:${i}`).slice(0,12);
                // Variants
                const skuData = rp?.data?.skuComponent || data.skuComponent;
                if (skuData?.productSKUPropertyList) {
                  for (const prop of skuData.productSKUPropertyList) {
                    const varGroup = {
                      name: prop.skuPropertyName,
                      values: (prop.skuPropertyValues || []).map(v => ({
                        value: v.propertyValueDisplayName || v.propertyValueName,
                        price: product.price,
                        stock: 10,
                        image: v.skuPropertyImagePath ? `https:${v.skuPropertyImagePath}` : '',
                      }))
                    };
                    if (varGroup.values.length > 0) product.variations.push(varGroup);
                    if (prop.skuPropertyName.toLowerCase().includes('color')) {
                      const imgMap = {};
                      for (const v of varGroup.values) if (v.image) imgMap[v.value] = v.image;
                      if (Object.keys(imgMap).length) product.variationImages['Color'] = imgMap;
                    }
                  }
                  product.hasVariations = product.variations.length > 0;
                }
              }
            } catch {}
          }

          // Fallbacks
          if (!product.title) {
            const m = html.match(/"subject":"([^"]+)"/);
            if (m) product.title = m[1];
          }
          if (!product.price) {
            const m = html.match(/"minAmount":\{"value":(\d+\.?\d*)/);
            if (m) product.price = m[1];
          }
          if (!product.images.length) {
            const imgs = [...html.matchAll(/"imageUrl":"(https?:\/\/ae[^"]+)"/g)].map(m=>m[1]);
            product.images = [...new Set(imgs)].slice(0,12);
          }
        }

        // ── WEBSTAURANTSTORE ─────────────────────────────────────────────────
        else if (url.includes('webstaurantstore.com')) {
          product.source = 'webstaurantstore';

          const titleM = html.match(/<h1[^>]*>([\s\S]*?)<\/h1>/i);
          if (titleM) product.title = titleM[1].replace(/<[^>]+>/g,'').trim();

          const priceM = html.match(/\$(\d+\.\d{2})/);
          if (priceM) product.price = priceM[1];

          const imgs = [...html.matchAll(/src="(https:\/\/cdn[0-9]*\.webstaurantstore\.com\/images\/[^"]+\.(?:jpg|jpeg|png|webp))"/gi)].map(m=>m[1]);
          product.images = [...new Set(imgs)].filter(i=>!i.includes('icon')&&!i.includes('logo')).slice(0,12);

          // Description
          const descM = html.match(/class="description[^"]*"[^>]*>([\s\S]*?)<\/div>/i);
          if (descM) product.description = descM[1].replace(/<[^>]+>/g,'').trim().slice(0,2000);

          // Variants (size/pack variations common on restaurant supply)
          const varTable = html.match(/<table[^>]*class="[^"]*variant[^"]*"[^>]*>([\s\S]*?)<\/table>/i) ||
                           html.match(/<div[^>]*class="[^"]*variant[^"]*"[^>]*>([\s\S]*?)<\/div>/i);
          if (varTable) {
            const varRows = [...varTable[1].matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/g)];
            const sizeVars = [];
            for (const row of varRows.slice(1)) {
              const cells = [...row[1].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/g)].map(m=>m[1].replace(/<[^>]+>/g,'').trim());
              if (cells.length >= 2 && cells[0]) {
                const priceCell = cells.find(c=>c.match(/\$[\d.]+/));
                sizeVars.push({ value:cells[0], price:priceCell?priceCell.replace(/[^0-9.]/g,''):product.price, stock:10, image:'' });
              }
            }
            if (sizeVars.length > 0) {
              product.variations.push({ name:'Size', values:sizeVars });
              product.hasVariations = true;
            }
          }

          // Item specifics
          const specRows = [...html.matchAll(/<tr[^>]*>[\s\S]*?<th[^>]*>([\s\S]*?)<\/th>[\s\S]*?<td[^>]*>([\s\S]*?)<\/td>[\s\S]*?<\/tr>/g)];
          for (const row of specRows.slice(0,10)) {
            const k = row[1].replace(/<[^>]+>/g,'').trim();
            const v = row[2].replace(/<[^>]+>/g,'').trim();
            if (k && v) product.aspects[k] = [v];
          }
        }

        return res.json({ success: true, product });
      } catch (e) {
        return res.json({ success: false, error: e.message, product });
      }
    }

    // ── Push listing (with variation support) ─────────────────────────────────
    if (action === 'push') {
      const { access_token, product } = body;
      if (!access_token || !product) return res.status(400).json({ error: 'Missing fields' });

      const groupSku = `DS-${Date.now()}-${Math.random().toString(36).slice(2,7).toUpperCase()}`;

      // ── NO VARIATIONS ──────────────────────────────────────────────────────
      if (!product.hasVariations || !product.variations || product.variations.length === 0) {
        const sku = groupSku;
        const invRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(sku)}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${access_token}` },
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: parseInt(product.quantity) || 10 } },
            condition: product.condition || 'NEW',
            product: {
              title: product.title.slice(0, 80),
              description: product.description || product.title,
              imageUrls: (product.images || []).slice(0, 12),
              aspects: product.aspects || {},
            },
          }),
        });
        if (!invRes.ok) return res.status(400).json({ error: 'Inventory failed', details: await invRes.text() });

        const offerPayload = buildOfferPayload(sku, product);
        const offerRes = await fetch(`${EBAY_API}/sell/inventory/v1/offer`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${access_token}` },
          body: JSON.stringify(offerPayload),
        });
        const offerData = await offerRes.json();
        if (!offerRes.ok) return res.status(400).json({ error: 'Offer failed', details: offerData });

        const pubRes = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerData.offerId}/publish`, {
          method: 'POST', headers: { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json' },
        });
        const pubData = await pubRes.json();
        return res.json({ success: true, sku, offerId: offerData.offerId, listingId: pubData.listingId });
      }

      // ── WITH VARIATIONS ────────────────────────────────────────────────────
      // Build all SKU combinations from variation groups
      const variationCombos = buildVariationCombos(product.variations);
      const createdSkus = [];

      for (const combo of variationCombos) {
        const varSku = `${groupSku}-${combo.map(v=>v.value.slice(0,4).replace(/\s/g,'')).join('-')}`;
        const varPrice = combo.reduce((p,v) => v.price || p, product.price);
        const varImages = getVariationImages(combo, product.variationImages, product.images);
        const varStock = combo.reduce((s,v) => v.stock !== undefined ? v.stock : s, parseInt(product.quantity)||10);

        // Build aspects for this variation
        const varAspects = { ...(product.aspects || {}) };
        for (const v of combo) varAspects[v.name] = [v.value];

        const invRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item/${encodeURIComponent(varSku)}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${access_token}` },
          body: JSON.stringify({
            availability: { shipToLocationAvailability: { quantity: Math.max(varStock, 0) } },
            condition: product.condition || 'NEW',
            product: {
              title: product.title.slice(0, 80),
              description: product.description || product.title,
              imageUrls: varImages.slice(0, 12),
              aspects: varAspects,
            },
          }),
        });
        if (invRes.ok) createdSkus.push({ sku: varSku, combo, price: varPrice, stock: varStock });
      }

      if (createdSkus.length === 0) return res.status(400).json({ error: 'No variation SKUs created' });

      // Create Inventory Item Group
      const groupPayload = {
        inventoryItemGroupKey: groupSku,
        title: product.title.slice(0, 80),
        description: product.description || product.title,
        imageUrls: (product.images || []).slice(0, 12),
        aspects: product.aspects || {},
        variesBy: {
          aspectsImageVariesBy: product.variationImages ? Object.keys(product.variationImages) : [],
          specifications: product.variations.map(v => ({
            name: v.name,
            values: v.values.map(val => val.value),
          })),
        },
      };

      // Add variation-specific images
      if (product.variationImages && Object.keys(product.variationImages).length > 0) {
        groupPayload.imageUrls = product.images.slice(0, 12);
      }

      const groupRes = await fetch(`${EBAY_API}/sell/inventory/v1/inventory_item_group/${encodeURIComponent(groupSku)}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${access_token}` },
        body: JSON.stringify(groupPayload),
      });
      if (!groupRes.ok) {
        const err = await groupRes.text();
        return res.status(400).json({ error: 'Group creation failed', details: err });
      }

      // Create offer for the group
      const groupOfferPayload = {
        ...buildOfferPayload(groupSku, product),
        inventoryItemGroupKey: groupSku,
      };
      delete groupOfferPayload.sku;

      const offerRes = await fetch(`${EBAY_API}/sell/inventory/v1/offer`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${access_token}` },
        body: JSON.stringify(groupOfferPayload),
      });
      const offerData = await offerRes.json();
      if (!offerRes.ok) return res.status(400).json({ error: 'Offer failed', details: offerData });

      const pubRes = await fetch(`${EBAY_API}/sell/inventory/v1/offer/${offerData.offerId}/publish`, {
        method: 'POST', headers: { Authorization: `Bearer ${access_token}`, 'Content-Type': 'application/json' },
      });
      const pubData = await pubRes.json();
      return res.json({ success: true, sku: groupSku, offerId: offerData.offerId, listingId: pubData.listingId, variationsCreated: createdSkus.length });
    }

    // ── Orders ────────────────────────────────────────────────────────────────
    if (action === 'orders') {
      const token = req.query.access_token || body.access_token;
      const r = await fetch(`${EBAY_API}/sell/fulfillment/v1/order?limit=50`, { headers: { Authorization: `Bearer ${token}` } });
      return res.json(await r.json());
    }

    // ── Listings ──────────────────────────────────────────────────────────────
    if (action === 'listings') {
      const token = req.query.access_token || body.access_token;
      const r = await fetch(`${EBAY_API}/sell/inventory/v1/offer?limit=100`, { headers: { Authorization: `Bearer ${token}` } });
      return res.json(await r.json());
    }

    return res.status(400).json({ error: `Unknown action: ${action}` });

  } catch (err) {
    console.error('[DropSync Error]', err);
    return res.status(500).json({ error: err.message });
  }
};

// ── HELPERS ───────────────────────────────────────────────────────────────────

function buildOfferPayload(sku, product) {
  const payload = {
    sku,
    marketplaceId: 'EBAY_US',
    format: 'FIXED_PRICE',
    listingDuration: 'GTC',
    pricingSummary: {
      price: { value: String(parseFloat(product.price || 0).toFixed(2)), currency: 'USD' },
    },
    categoryId: product.categoryId || '9355',
    merchantLocationKey: 'default',
  };
  if (process.env.EBAY_FULFILLMENT_POLICY_ID) payload.fulfillmentPolicyId = process.env.EBAY_FULFILLMENT_POLICY_ID;
  if (process.env.EBAY_PAYMENT_POLICY_ID)     payload.paymentPolicyId     = process.env.EBAY_PAYMENT_POLICY_ID;
  if (process.env.EBAY_RETURN_POLICY_ID)       payload.returnPolicyId       = process.env.EBAY_RETURN_POLICY_ID;
  return payload;
}

function buildVariationCombos(variations) {
  if (!variations || variations.length === 0) return [];
  let combos = [[]];
  for (const varGroup of variations) {
    const newCombos = [];
    for (const existing of combos) {
      for (const val of (varGroup.values || [])) {
        newCombos.push([...existing, { name: varGroup.name, value: val.value, price: val.price, stock: val.stock, image: val.image }]);
      }
    }
    combos = newCombos;
  }
  return combos;
}

function getVariationImages(combo, variationImages, fallbackImages) {
  const imgs = [];
  // Try to get color-specific image first
  if (variationImages) {
    for (const v of combo) {
      const groupImgs = variationImages[v.name];
      if (groupImgs && groupImgs[v.value]) imgs.push(groupImgs[v.value]);
    }
  }
  // Check if combo has a direct image
  for (const v of combo) if (v.image) imgs.push(v.image);
  // Fallback to main images
  if (imgs.length === 0) return fallbackImages || [];
  return [...new Set([...imgs, ...(fallbackImages || [])])];
}
