// indexer.cjs
// ============================================================
// Multi-Indexer — procesa varios contratos con cursores
// independientes y un handler por tipo de contrato.
//
// Contratos actuales:
//   · CukiesMarketplace   → tabla orders / trades
//   · CukiePoolNftVault   → tabla vault_positions (kind='pool')
//   · CukiMasterNftVault  → tabla vault_positions (kind='master')
// ============================================================

const { ethers } = require('ethers');
const { createClient } = require('@supabase/supabase-js');

// ------------------------------------------------------------
// Config
// ------------------------------------------------------------
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const BSCTRACE_API_KEY = process.env.BSCTRACE_API_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !BSCTRACE_API_KEY) {
  console.error('[Indexer] Faltan variables de entorno');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

const RPC_URL = `https://bsc-mainnet.nodereal.io/v1/${BSCTRACE_API_KEY}`;
const provider = new ethers.JsonRpcProvider(RPC_URL, 56, { staticNetwork: true });

// ------------------------------------------------------------
// ABIs
// ------------------------------------------------------------

// Marketplace V2
const MARKET_ABI = [
  'event OrderCreated(bytes32 indexed orderId, address indexed collection, uint256 indexed tokenId, address seller, uint256 ukiPrice, uint64 expiresAt, uint64 nonce, uint16 feeBps)',
  'event OrderFilled(bytes32 indexed orderId, address indexed buyer, address indexed paymentToken, uint256 paymentAmount, uint256 feeAmount, uint256 ukiPrice)',
  'event OrderCancelled(bytes32 indexed orderId, address indexed seller)',
  'event OrderExpired(bytes32 indexed orderId)',
  'event OrderInvalidated(bytes32 indexed orderId, bytes32 indexed reason)',
];

// CukiePoolNftVault — Deposited tiene 9 parámetros (incluye periodIds, activationAt, calendarVersion)
const VAULT_POOL_ABI = [
  'event Deposited(address indexed collection, uint256 indexed tokenId, address indexed beneficiary, uint64 depositEpoch, uint64 depositedAt, uint64 depositPeriodId, uint64 activationAt, uint64 activationPeriodId, uint32 calendarVersion)',
  'event Withdrawn(address indexed collection, uint256 indexed tokenId, address indexed beneficiary, uint64 depositEpoch, uint64 withdrawnAt)',
];

// CukiMasterNftVault — Deposited tiene solo 5 parámetros, con uint256 en vez de uint64
const VAULT_MASTER_ABI = [
  'event Deposited(address indexed collection, uint256 indexed tokenId, address indexed beneficiary, uint256 depositEpoch, uint256 depositedAt)',
  'event Withdrawn(address indexed collection, uint256 indexed tokenId, address indexed beneficiary, uint256 depositEpoch, uint256 withdrawnAt)',
];

// ------------------------------------------------------------
// Definición de contratos a indexar
// ------------------------------------------------------------
// `cursorKey` permite preservar cursores existentes y evitar re-escaneos.
// Si no se especifica, se usa `idx_<name>` como key por defecto.
const CONTRACTS = {
  marketplace_new: {
    address: '0x5DC15A1b154c6854850205417171275092EF4233',
    fromBlock: 125_680_296,
    cursorKey: 'marketplace_new',       // ← preserva el cursor antiguo
    abi: MARKET_ABI,
    handler: 'marketplace',
    kind: 'new',
  },
  vault_pool: {
    address: '0x5266115a017A3a6FF456A7dE2E2caE87462601f5',
    fromBlock: 123_166_139,
    cursorKey: 'idx_vault_pool',        // ← cursor nuevo
    abi: VAULT_POOL_ABI,
    handler: 'vault',
    kind: 'pool',
  },
  vault_master: {
    address: '0xcE90b2bbC286c11dB020f25df803b99F6FDe4ba7',
    fromBlock: 122_802_953,
    cursorKey: 'idx_vault_master',      // ← cursor nuevo
    abi: VAULT_MASTER_ABI,
    handler: 'vault',
    kind: 'master',
  },
};

// ------------------------------------------------------------
// Ajustes de rendimiento
// ------------------------------------------------------------
const REFRESH_STATES_INTERVAL_HOURS = 3;
const STATE_REFRESH_DELAY_MS = 500;
const BLOCKS_PER_CHUNK = 10_000;
const RPC_DELAY_MS = 500;

// 4 min por contrato. Con 3 contratos, ~12 min como máximo por run.
// Si el cron es cada 10 min, hay solapamiento: la siguiente ejecución
// continúa donde lo dejó la anterior por el cursor persistido.
const MAX_RUNTIME_MS = 4 * 60 * 1000;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// ------------------------------------------------------------
// Estado del indexer (persistido en Supabase)
// ------------------------------------------------------------
async function getLastBlock(key, fallback = 0) {
  const { data, error } = await supabase
    .from('indexer_state')
    .select('last_block')
    .eq('key', key)
    .maybeSingle();
  if (error || !data) return fallback;
  return Number(data.last_block);
}

async function setLastBlock(key, block) {
  await supabase.from('indexer_state').upsert({
    key,
    last_block: block,
    updated_at: new Date().toISOString(),
  });
}

// ------------------------------------------------------------
// Control de frecuencia del refresco de estados (solo marketplace)
// ------------------------------------------------------------
// ------------------------------------------------------------
// Detección de órdenes expiradas que necesitan reconciliación
// ------------------------------------------------------------
// Consulta la DB buscando órdenes en state=1 cuyo expires_at ya pasó.
// Si hay alguna, significa que el contrato las considera EXPIRED pero
// la DB aún no se ha enterado (falta el evento OrderExpired on-chain o
// el refresh periódico no ha corrido todavía).
// Devuelve el array de order_ids a reconciliar (o [] si no hay ninguna).
async function findStaleExpiredOrders() {
  const now = Math.floor(Date.now() / 1000);

  const { data, error } = await supabase
    .from('orders')
    .select('order_id')
    .eq('state', 1)
    .eq('marketplace', 'new')
    .gt('expires_at', 0)
    .lt('expires_at', now)
    .limit(200);   // límite por run para no pasarse de tiempo

  if (error) {
    console.warn('[Indexer] findStaleExpiredOrders fallo:', error.message);
    return [];
  }

  return (data || []).map((r) => r.order_id);
}

// ------------------------------------------------------------
// Refresh quirúrgico de órdenes concretas
// ------------------------------------------------------------
// Solo llama a orderState() para las órdenes que realmente lo necesitan.
// Mucho más barato que recorrer TODAS las órdenes activas.
async function refreshSpecificOrders(config, orderIds) {
  if (!orderIds || orderIds.length === 0) return { checked: 0, updated: 0 };

  console.log(`[marketplace] Reconciliando ${orderIds.length} órdenes expiradas…`);

  const market = new ethers.Contract(
    config.address,
    ['function orderState(bytes32 orderId) view returns (uint8)'],
    provider
  );

  let updated = 0;
  const now = new Date().toISOString();

  for (const orderId of orderIds) {
    try {
      const state = Number(await market.orderState(orderId));
      if (state !== 1) {
        const { error } = await supabase
          .from('orders')
          .update({ state, updated_at: now })
          .eq('order_id', orderId);
        if (!error) updated++;
      }
    } catch (e) {
      console.warn(`[marketplace] orderState fallo ${orderId.slice(0, 10)}…:`, e.message);
    }
    await sleep(STATE_REFRESH_DELAY_MS);
  }

  console.log(`[marketplace] Reconciliación: ${updated}/${orderIds.length} actualizadas`);
  return { checked: orderIds.length, updated };
}

async function markStatesRefreshed() {
  await supabase.from('indexer_state').upsert({
    key: 'state_refresh',
    last_block: 0,
    updated_at: new Date().toISOString(),
  });
}

// ------------------------------------------------------------
// Cache de timestamps de bloques
// ------------------------------------------------------------
const blockTimestampCache = new Map();

async function getBlockTimestamp(blockNumber) {
  if (blockTimestampCache.has(blockNumber)) {
    return blockTimestampCache.get(blockNumber);
  }
  try {
    const block = await provider.getBlock(blockNumber);
    const ts = block?.timestamp ? new Date(block.timestamp * 1000).toISOString() : null;
    blockTimestampCache.set(blockNumber, ts);
    return ts;
  } catch (e) {
    console.warn(`[Indexer] No se pudo leer bloque ${blockNumber}:`, e.message);
    blockTimestampCache.set(blockNumber, null);
    return null;
  }
}

// ============================================================
// HANDLER: Marketplace
// ============================================================
async function processMarketplaceChunk(name, contract, fromBlock, toBlock) {
  const queries = [
    { name: 'OrderCreated', filter: contract.filters.OrderCreated() },
    { name: 'OrderFilled', filter: contract.filters.OrderFilled() },
    { name: 'OrderCancelled', filter: contract.filters.OrderCancelled() },
    { name: 'OrderExpired', filter: contract.filters.OrderExpired() },
    { name: 'OrderInvalidated', filter: contract.filters.OrderInvalidated() },
  ];

  const eventResults = [];
  for (let i = 0; i < queries.length; i++) {
    const { name: qName, filter } = queries[i];
    try {
      const events = await contract.queryFilter(filter, fromBlock, toBlock);
      eventResults.push(events);
    } catch (e) {
      console.error(`[${name}] Error en query ${qName} (${fromBlock}-${toBlock}):`, e.message);
      eventResults.push([]);
    }
    if (i < queries.length - 1) {
      await sleep(RPC_DELAY_MS);
    }
  }

  const [created, filled, cancelled, expired, invalidated] = eventResults;

  const totalEvents = created.length + filled.length + cancelled.length + expired.length + invalidated.length;
  if (totalEvents === 0) return;

  console.log(
    `[${name}] Chunk ${fromBlock}-${toBlock}: ${created.length}c ${filled.length}f ` +
    `${cancelled.length}x ${expired.length}e ${invalidated.length}i`
  );

  // 1. OrderCreated → upsert en orders
  if (created.length > 0) {
    const rows = created.map((ev) => ({
      order_id: ev.args.orderId,
      collection: ev.args.collection.toLowerCase(),
      token_id: ev.args.tokenId.toString(),
      seller: ev.args.seller.toLowerCase(),
      uki_price: ev.args.ukiPrice.toString(),
      expires_at: Number(ev.args.expiresAt),
      fee_bps: Number(ev.args.feeBps),
      marketplace: 'new',
      state: 1,
      updated_at: new Date().toISOString(),
    }));

    const { error } = await supabase.from('orders').upsert(rows, { onConflict: 'order_id' });
    if (error) console.error(`[${name}] Error insertando orders:`, error.message);
  }

  // 2. OrderFilled → actualizar orders + insertar trades
  if (filled.length > 0) {
    const orderIds = filled.map((ev) => ev.args.orderId);
    const { data: existingOrders, error: errOrders } = await supabase
      .from('orders')
      .select('order_id, collection, token_id, seller')
      .in('order_id', orderIds);

    if (errOrders) console.error(`[${name}] Error leyendo orders para trades:`, errOrders.message);

    const ordersMap = new Map((existingOrders || []).map((o) => [o.order_id, o]));

    const tradeRows = [];
    for (const ev of filled) {
      const order = ordersMap.get(ev.args.orderId);
      if (!order) {
        console.warn(`[${name}] Trade sin orden conocida: ${ev.args.orderId}`);
        continue;
      }
      const ts = await getBlockTimestamp(ev.blockNumber);
      tradeRows.push({
        order_id: ev.args.orderId,
        collection: order.collection,
        token_id: order.token_id,
        seller: order.seller,
        buyer: ev.args.buyer.toLowerCase(),
        payment_token: ev.args.paymentToken.toLowerCase(),
        payment_amount: ev.args.paymentAmount.toString(),
        fee_amount: ev.args.feeAmount.toString(),
        uki_price: ev.args.ukiPrice.toString(),
        block_number: ev.blockNumber,
        tx_hash: ev.transactionHash,
        timestamp: ts,
        marketplace: 'new',
      });
    }

    if (tradeRows.length > 0) {
      // Upsert idempotente. Si el cliente ya notificó vía /api/order-notify,
      // esta fila ya existe y se actualiza en lugar de duplicar.
      const { error } = await supabase
        .from('trades')
        .upsert(tradeRows, { onConflict: 'order_id' });
      if (error) console.error(`[${name}] Error upsert trades:`, error.message);
    }

    const { error: errUpdate } = await supabase
      .from('orders')
      .update({ state: 2, updated_at: new Date().toISOString() })
      .in('order_id', orderIds);
    if (errUpdate) console.error(`[${name}] Error marcando orders como sold:`, errUpdate.message);
  }

  // 3. Estados finales (cancelada / expirada / invalidada)
  const updateState = async (events, state) => {
    if (events.length === 0) return;
    const ids = events.map((ev) => ev.args.orderId);
    const { error } = await supabase
      .from('orders')
      .update({ state, updated_at: new Date().toISOString() })
      .in('order_id', ids);
    if (error) console.error(`[${name}] Error actualizando state=${state}:`, error.message);
  };

  await Promise.all([
    updateState(cancelled, 3),
    updateState(expired, 4),
    updateState(invalidated, 5),
  ]);
}

// ============================================================
// HANDLER: Vault (pool o master)
// ============================================================
// Los dos vaults tienen Deposited con firma distinta, pero en JS
// los args se leen igual: ev.args.depositEpoch.toString() funciona
// tanto con uint64 (pool) como con uint256 (master).
async function processVaultChunk(name, config, contract, fromBlock, toBlock) {
  let deposited = [];
  let withdrawn = [];

  try {
    deposited = await contract.queryFilter(contract.filters.Deposited(), fromBlock, toBlock);
  } catch (e) {
    console.error(`[${name}] Deposited fallo (${fromBlock}-${toBlock}):`, e.message);
  }
  await sleep(RPC_DELAY_MS);

  try {
    withdrawn = await contract.queryFilter(contract.filters.Withdrawn(), fromBlock, toBlock);
  } catch (e) {
    console.error(`[${name}] Withdrawn fallo (${fromBlock}-${toBlock}):`, e.message);
  }

  if (deposited.length === 0 && withdrawn.length === 0) return;

  console.log(`[${name}] Chunk ${fromBlock}-${toBlock}: ${deposited.length}d ${withdrawn.length}w`);

  // 1. Deposited → upsert en vault_positions
  if (deposited.length > 0) {
    const rows = [];
    for (const ev of deposited) {
      const ts = await getBlockTimestamp(ev.blockNumber);
      rows.push({
        vault_address: config.address.toLowerCase(),
        vault_kind: config.kind,
        collection: ev.args.collection.toLowerCase(),
        token_id: ev.args.tokenId.toString(),
        beneficiary: ev.args.beneficiary.toLowerCase(),
        deposit_epoch: ev.args.depositEpoch.toString(),
        deposited_at: ts,
        withdrawn_at: null,
        active: true,
        updated_at: new Date().toISOString(),
      });
    }

    const { error } = await supabase
      .from('vault_positions')
      .upsert(rows, { onConflict: 'vault_address,collection,token_id,deposit_epoch' });
    if (error) console.error(`[${name}] Error upsert deposits:`, error.message);
  }

  // 2. Withdrawn → marcar inactive (no borramos: histórico)
  if (withdrawn.length > 0) {
    for (const ev of withdrawn) {
      const ts = await getBlockTimestamp(ev.blockNumber);
      const { error } = await supabase
        .from('vault_positions')
        .update({
          active: false,
          withdrawn_at: ts,
          updated_at: new Date().toISOString(),
        })
        .eq('vault_address', config.address.toLowerCase())
        .eq('collection', ev.args.collection.toLowerCase())
        .eq('token_id', ev.args.tokenId.toString())
        .eq('deposit_epoch', ev.args.depositEpoch.toString());
      if (error) console.error(`[${name}] Error marcando withdrawn:`, error.message);
    }
  }
}

// ============================================================
// Dispatcher de handlers
// ============================================================
async function processChunk(name, config, contract, fromBlock, toBlock) {
  if (config.handler === 'marketplace') {
    return processMarketplaceChunk(name, contract, fromBlock, toBlock);
  }
  if (config.handler === 'vault') {
    return processVaultChunk(name, config, contract, fromBlock, toBlock);
  }
  throw new Error(`Handler desconocido: ${config.handler}`);
}

// ------------------------------------------------------------
// Loop principal de un contrato (con cursor y time budget)
// ------------------------------------------------------------
async function processContract(name, config) {
  const stateKey = config.cursorKey || `idx_${name}`;
  const contract = new ethers.Contract(config.address, config.abi, provider);
  const startedAt = Date.now();

  const currentBlock = await provider.getBlockNumber();
  const lastBlock = await getLastBlock(stateKey, config.fromBlock - 1);
  let cursor = Math.max(lastBlock + 1, config.fromBlock);

  if (cursor > currentBlock) {
    console.log(`[${name}] Al día (bloque ${lastBlock} / ${currentBlock})`);
    return { name, chunksProcessed: 0, lastBlock, currentBlock, upToDate: true };
  }

  console.log(
    `[${name}] Sincronizando desde bloque ${cursor} → ${currentBlock} ` +
    `(faltan ${currentBlock - cursor + 1} bloques)`
  );

  let chunksProcessed = 0;

  while (cursor <= currentBlock) {
    if (Date.now() - startedAt > MAX_RUNTIME_MS) {
      console.log(
        `[${name}] Tiempo agotado. Progreso guardado en bloque ${cursor - 1}. ` +
        `Continuará en la próxima ejecución.`
      );
      break;
    }

    const chunkEnd = Math.min(cursor + BLOCKS_PER_CHUNK - 1, currentBlock);

    try {
      await processChunk(name, config, contract, cursor, chunkEnd);
    } catch (e) {
      console.error(`[${name}] FALLO en chunk ${cursor}-${chunkEnd}:`, e.message);
      // No guardamos progreso para que el próximo run reintente este chunk
      throw e;
    }

    await setLastBlock(stateKey, chunkEnd);
    chunksProcessed++;
    cursor = chunkEnd + 1;

    if (cursor <= currentBlock) {
      await sleep(RPC_DELAY_MS);
    }
  }

  const finalBlock = cursor - 1;
  const upToDate = finalBlock >= currentBlock;

  console.log(
    `[${name}] OK · ${chunksProcessed} chunks procesados · último bloque ${finalBlock}` +
    `${upToDate ? ' ✅' : ' ⏳'}`
  );

  return { name, chunksProcessed, lastBlock: finalBlock, currentBlock, upToDate };
}

// ------------------------------------------------------------
// Refresco on-chain de órdenes activas (solo marketplace)
// ------------------------------------------------------------
async function refreshActiveOrdersState(config) {
  const { data: activeOrders, error } = await supabase
    .from('orders')
    .select('order_id')
    .eq('state', 1)
    .eq('marketplace', config.kind);

  if (error) {
    console.error(`[marketplace] Error leyendo órdenes activas:`, error.message);
    return { checked: 0, updated: 0 };
  }

  if (!activeOrders || activeOrders.length === 0) {
    console.log(`[marketplace] Sin órdenes activas para refrescar`);
    return { checked: 0, updated: 0 };
  }

  console.log(`[marketplace] Refrescando ${activeOrders.length} órdenes activas…`);

  const market = new ethers.Contract(
    config.address,
    ['function orderState(bytes32 orderId) view returns (uint8)'],
    provider
  );

  let updated = 0;
  const now = new Date().toISOString();

  for (const row of activeOrders) {
    try {
      const state = Number(await market.orderState(row.order_id));
      if (state !== 1) {
        const { error: errUpd } = await supabase
          .from('orders')
          .update({ state, updated_at: now })
          .eq('order_id', row.order_id);
        if (errUpd) {
          console.error(`[marketplace] Error actualizando ${row.order_id}:`, errUpd.message);
        } else {
          updated++;
        }
      }
    } catch (e) {
      console.warn(`[marketplace] orderState fallo para ${row.order_id.slice(0, 10)}…:`, e.message);
    }
    await sleep(STATE_REFRESH_DELAY_MS);
  }

  console.log(`[marketplace] Refresco completado: ${updated}/${activeOrders.length} actualizadas`);
  return { checked: activeOrders.length, updated };
}

// ------------------------------------------------------------
// Main
// ------------------------------------------------------------
(async function main() {
  const startedAt = Date.now();
  console.log(`[Indexer] Inicio ${new Date().toISOString()}`);

  let hadError = false;

  // 1. Procesar todos los contratos configurados
  for (const [name, config] of Object.entries(CONTRACTS)) {
    try {
      const result = await processContract(name, config);
      console.log(`[${name}] Resultado:`, JSON.stringify(result));
    } catch (e) {
      hadError = true;
      console.error(`[${name}] FALLO:`, e.message);
    }
  }

// 2. Reconciliación de órdenes expiradas (siempre, es barato)
try {
  const marketCfg = CONTRACTS.marketplace_new;
  if (marketCfg) {
    const staleOrders = await findStaleExpiredOrders();

    if (staleOrders.length > 0) {
      // Hay órdenes caducadas en la DB → reconciliar YA (quirúrgico)
      await refreshSpecificOrders(marketCfg, staleOrders);
    } else {
      console.log('[Indexer] Sin órdenes expiradas pendientes de reconciliar');
    }
  }
} catch (e) {
  console.error('[Indexer] Error en reconciliación de expiradas:', e.message);
}

// 3. Refresh completo periódico (cada 3h, red de seguridad)
//    Cubre casos raros: órdenes canceladas, invalidadas, o expiradas
//    cuyo expires_at es 0/Null (sin caducidad) pero el contrato sí las cerró.
try {
  if (await shouldRefreshStates()) {
    console.log('[Indexer] Full refresh periódico (cada 3h)…');
    const marketCfg = CONTRACTS.marketplace_new;
    if (marketCfg) {
      try {
        await refreshActiveOrdersState(marketCfg);
      } catch (e) {
        console.error('[marketplace] Fallo en full refresh:', e.message);
      }
    }
    await markStatesRefreshed();
    console.log('[Indexer] Full refresh completado ✅');
  }
} catch (e) {
  console.error('[Indexer] Error en full refresh:', e.message);
}

  const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
  console.log(`[Indexer] Fin en ${elapsed}s`);

  process.exit(hadError ? 1 : 0);
})();
