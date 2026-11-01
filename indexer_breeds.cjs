// indexer_breeds.cjs
// ============================================================
// Breeds Indexer
//
// 1) BSC · por cada wallet en `user_wallets`, lee on-chain:
//    getAllBreedsOwner → breeds(id) → upsert en `breedings`
//
// 2) BSC · indexa TODOS los eventos BreedFinish del contrato
//    → upsert en `all_breeds` (chain='bsc')
//
// 3) TRON · iterar breeds(0..4248) una sola vez (backfill)
//    → upsert en `all_breeds` (chain='tron')
//    Después solo revisa la cola (últimos 20 slots) por si
//    apareciera alguna nueva (improbable: TRON paró en 2023).
//
// Ejecución recomendada: cada 2-4h vía GitHub Actions.
// ============================================================

const { ethers } = require('ethers');
const { createClient } = require('@supabase/supabase-js');

// ------------------------------------------------------------
// Env
// ------------------------------------------------------------
const SUPABASE_URL         = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const BSCTRACE_API_KEY     = process.env.BSCTRACE_API_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !BSCTRACE_API_KEY) {
  console.error('[Breeds] Faltan variables de entorno');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

const RPC_URL = `https://bsc-mainnet.nodereal.io/v1/${BSCTRACE_API_KEY}`;
const provider = new ethers.JsonRpcProvider(RPC_URL, 56, { staticNetwork: true });

// ------------------------------------------------------------
// Constantes BSC
// ------------------------------------------------------------
const BREEDING   = '0x39be8c4fa342c5f3c10d7c16941a0946d29ade4a';
const MULTICALL3 = '0xcA11bde05977b3631167028862bE2a173976CA11';

const BREEDING_ABI = [
  'function getNumBreedsByCukie(uint256 _cukieId) view returns (uint256)',
  'function getAllBreedsOwner(address _user) view returns (uint256[])',
  'function breeds(uint256) view returns (uint256 parent1, uint256 parent2, uint256 start_at, uint256 finish_at, address owner, bool completed, uint256 result)',
];

const MULTICALL_ABI = [
  'function aggregate3((address target, bool allowFailure, bytes callData)[] calls) payable returns ((bool success, bytes returnData)[])',
];

const BREED_FINISH_ABI = [
  'event BreedFinish(address indexed user, uint256 indexed parent1, uint256 indexed parent2, uint256 result, uint256 date)',
];

// ------------------------------------------------------------
// Constantes TRON
// ------------------------------------------------------------
const TRON_BREEDING_BASE58 = 'TXrvQKgzWpsMkp9ebiF1uXNPRgKxNanB9S';
const TRON_KNOWN_LAST      = 4248;   // último breed_id confirmado (2023-04-10)
const TRON_SAFETY_BUFFER   = 20;     // cola adicional a revisar
const TRON_ZERO_ADDRESS    = 'T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb';
const TRONGRID_API_KEY     = '718a45cc-662e-459d-9a47-ecb0bc8d2d66';

const TRON_BREEDING_ABI = [{
  inputs:  [{ name: '_breed', type: 'uint256' }],
  name:    'breeds',
  outputs: [
    { name: 'parent1',   type: 'uint256' },
    { name: 'parent2',   type: 'uint256' },
    { name: 'start_at',  type: 'uint256' },
    { name: 'finish_at', type: 'uint256' },
    { name: 'owner',     type: 'address' },
    { name: 'completed', type: 'bool'    },
    { name: 'result',    type: 'uint256' },
  ],
  stateMutability: 'view',
  type: 'function',
}];

// ------------------------------------------------------------
// Config BSC
// ------------------------------------------------------------
const WALLETS_PER_CHUNK = 100;
const BREEDS_PER_CHUNK  = 150;
const CHUNK_DELAY_MS    = 250;

const EVENT_CHUNK       = 25_000;
const EVENT_CHUNK_DELAY = 300;
const EVENT_FROM_BLOCK  = 17_000_000;

// ------------------------------------------------------------
// Config TRON
// ------------------------------------------------------------
const TRON_CONCURRENCY = 12;    // llamadas simultáneas a TronGrid
const TRON_DELAY_MS    = 80;  // entre tandas (evita 429)

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ============================================================
// 1) Wallets conocidas
// ============================================================
async function loadAllWallets() {
  const wallets = [];
  const PAGE = 1000;
  let from = 0;

  while (true) {
    const { data, error } = await supabase
      .from('user_wallets')
      .select('address')
      .range(from, from + PAGE - 1);

    if (error) {
      console.error('[Breeds] error user_wallets:', error.message);
      break;
    }
    if (!data || data.length === 0) break;

    for (const r of data) wallets.push(r.address.toLowerCase());
    if (data.length < PAGE) break;
    from += PAGE;
  }
  return [...new Set(wallets)];
}

// ============================================================
// 2) getAllBreedsOwner por wallet (multicall)
// ============================================================
async function fetchAllBreedIds(wallets, breeding, multicall) {
  const allIds = new Set();

  for (let i = 0; i < wallets.length; i += WALLETS_PER_CHUNK) {
    const batch = wallets.slice(i, i + WALLETS_PER_CHUNK);
    const calls = batch.map((addr) => ({
      target: BREEDING,
      allowFailure: true,
      callData: breeding.interface.encodeFunctionData('getAllBreedsOwner', [addr]),
    }));

    try {
      const results = await multicall.aggregate3.staticCall(calls);
      for (let j = 0; j < results.length; j++) {
        if (!results[j].success) continue;
        const ids = breeding.interface.decodeFunctionResult(
          'getAllBreedsOwner',
          results[j].returnData
        )[0];
        for (const id of ids) allIds.add(String(id));
      }
    } catch (e) {
      console.warn(`[Breeds] getAllBreedsOwner batch ${i} falló:`, e.message);
    }

    console.log(
      `[Breeds] Wallets procesadas: ${Math.min(i + WALLETS_PER_CHUNK, wallets.length)}/${wallets.length}` +
      ` · IDs totales: ${allIds.size}`
    );
    if (i + WALLETS_PER_CHUNK < wallets.length) await sleep(CHUNK_DELAY_MS);
  }
  return [...allIds];
}

// ============================================================
// 3) breeds(id) por cada ID (multicall)
// ============================================================
async function fetchBreedRows(ids, breeding, multicall) {
  const rows = [];

  for (let i = 0; i < ids.length; i += BREEDS_PER_CHUNK) {
    const batch = ids.slice(i, i + BREEDS_PER_CHUNK);
    const calls = batch.map((id) => ({
      target: BREEDING,
      allowFailure: true,
      callData: breeding.interface.encodeFunctionData('breeds', [id]),
    }));

    try {
      const results = await multicall.aggregate3.staticCall(calls);
      for (let j = 0; j < results.length; j++) {
        if (!results[j].success) continue;
        const d = breeding.interface.decodeFunctionResult('breeds', results[j].returnData);
        const owner = d[4];
        if (!owner || owner === ethers.ZeroAddress) continue;

        const completed = !!d[5];
        const result = d[6];

        rows.push({
          breed_id: String(batch[j]),
          user_wallet: owner.toLowerCase(),
          parent1_id: d[0].toString(),
          parent2_id: d[1].toString(),
          started_at: new Date(Number(d[2]) * 1000).toISOString(),
          finish_at: new Date(Number(d[3]) * 1000).toISOString(),
          status: completed ? 'bred' : 'started',
          bred_token_id: completed && result > 0n ? result.toString() : null,
        });
      }
    } catch (e) {
      console.warn(`[Breeds] breeds batch ${i} falló:`, e.message);
    }

    console.log(`[Breeds] Rows preparadas: ${rows.length}/${ids.length}`);
    if (i + BREEDS_PER_CHUNK < ids.length) await sleep(CHUNK_DELAY_MS);
  }
  return rows;
}

// ============================================================
// 4) Upsert en `breedings` (BSC)
// ============================================================
async function upsertBreedRows(rows) {
  const CHUNK = 500;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    const { error } = await supabase
      .from('breedings')
      .upsert(chunk, { onConflict: 'breed_id' });

    if (error) {
      console.error(`[Breeds] upsert breedings lote ${i} falló:`, error.message);
      throw error;
    }
    console.log(`[Breeds] Upsert OK ${i + chunk.length}/${rows.length}`);
  }
}

// ============================================================
// 5) BSC · BreedFinish → all_breeds
// ============================================================
async function indexBscBreedFinish() {
  const stateKey = 'breed_finish';

  const { data: stateRow, error: stateErr } = await supabase
    .from('indexer_state')
    .select('last_block')
    .eq('key', stateKey)
    .maybeSingle();

  if (stateErr) console.warn('[Breeds] indexer_state SELECT falló:', stateErr.message);

  const savedLast = stateRow?.last_block ? Number(stateRow.last_block) : 0;
  let fromBlock = savedLast > 0 ? savedLast + 1 : EVENT_FROM_BLOCK;

  const head = await provider.getBlockNumber();
  if (fromBlock > head) {
    console.log(`[Breeds] BSC all_breeds ya al día (${fromBlock} > ${head})`);
    return;
  }

  console.log(
    `[Breeds] BSC all_breeds: ${fromBlock} → ${head} ` +
    `(${head - fromBlock + 1} bloques · chunks de ${EVENT_CHUNK})`
  );

  const contract = new ethers.Contract(BREEDING, BREED_FINISH_ABI, provider);
  let totalInserted = 0;
  let chunkCount = 0;

  while (fromBlock <= head) {
    const toBlock = Math.min(fromBlock + EVENT_CHUNK - 1, head);
    let evs = [];

    try {
      evs = await contract.queryFilter('BreedFinish', fromBlock, toBlock);
    } catch (e) {
      console.warn(`[Breeds] queryFilter ${fromBlock}→${toBlock} falló:`, e.message);
      fromBlock = toBlock + 1;
      await sleep(EVENT_CHUNK_DELAY);
      continue;
    }

    if (evs.length > 0) {
      const rows = evs
        .filter((ev) => ev?.args?.result != null)
        .map((ev) => ({
          breed_result_id: String(ev.args.result),
          parent1_id:      String(ev.args.parent1),
          parent2_id:      String(ev.args.parent2),
          owner:           ev.args.user.toLowerCase(),
          bred_at:         new Date(Number(ev.args.date) * 1000).toISOString(),
          chain:           'bsc',
        }));

      if (rows.length > 0) {
        const { error } = await supabase
          .from('all_breeds')
          .upsert(rows, { onConflict: 'breed_result_id' });

        if (error) {
          console.error(
            `[Breeds] upsert all_breeds (BSC) ${fromBlock}→${toBlock} falló:`,
            error.message
          );
        } else {
          totalInserted += rows.length;
        }
      }
    }

    chunkCount++;
    console.log(
      `[Breeds] BSC all_breeds ${fromBlock}→${toBlock}: ` +
      `${evs.length} finishes · total ${totalInserted} · chunk ${chunkCount}`
    );

    await supabase.from('indexer_state').upsert({
      key: stateKey,
      last_block: toBlock,
      updated_at: new Date().toISOString(),
    });

    fromBlock = toBlock + 1;
    if (fromBlock <= head) await sleep(EVENT_CHUNK_DELAY);
  }

  console.log(`[Breeds] ✅ BSC all_breeds · ${totalInserted} finishes nuevos`);
}

// ============================================================
// 6) TRON · breeds(0..N) → all_breeds
// ============================================================
async function indexTronBreeds() {
  // Carga diferida de tronweb para no romper si no está instalado
  let TronWeb;
  try {
    ({ TronWeb } = require('tronweb'));
  } catch {
    console.warn(
      '[Breeds] TRON: `tronweb` no instalado — skipping. ' +
      'Ejecuta `npm install tronweb` para habilitar la fase TRON.'
    );
    return;
  }

  // ¿Ya hicimos el backfill completo?
  const { data: stateRow } = await supabase
    .from('indexer_state')
    .select('last_block')
    .eq('key', 'tron_breed_backfill_done')
    .maybeSingle();

  const alreadyDone = !!stateRow?.last_block;

  const startId = alreadyDone ? TRON_KNOWN_LAST + 1 : 0;
  const maxId   = TRON_KNOWN_LAST + TRON_SAFETY_BUFFER;

  if (startId > maxId) {
    console.log('[Breeds] TRON: nada que revisar');
    return;
  }

  console.log(
    `[Breeds] TRON: ${alreadyDone ? 'tail check' : 'backfill completo'} ` +
    `${startId} → ${maxId} (${maxId - startId + 1} breeds)`
  );

  const TRONGRID_API_KEY = '718a45cc-662e-459d-9a47-ecb0bc8d2d66';

  const tronWeb = new TronWeb({
    fullHost: 'https://api.trongrid.io',
    headers: { 'TRON-PRO-API-KEY': TRONGRID_API_KEY },
  });

  const contract = await tronWeb.contract(TRON_BREEDING_ABI, TRON_BREEDING_BASE58);

  const rows = [];
  let processed = 0;
  let valid = 0;

  for (let batchStart = startId; batchStart <= maxId; batchStart += TRON_CONCURRENCY) {
    const ids = [];
    for (let k = 0; k < TRON_CONCURRENCY && batchStart + k <= maxId; k++) {
      ids.push(batchStart + k);
    }

    const results = await Promise.all(ids.map(async (id) => {
      try {
        const b = await contract.breeds(id).call();
        return { id, data: b, error: null };
      } catch (e) {
        return { id, data: null, error: e.message };
      }
    }));

    for (const { id, data, error } of results) {
      processed++;
      if (error) {
        console.warn(`[Breeds] TRON breeds(${id}) error:`, error);
        continue;
      }

      const owner = String(data.owner || '');
      if (!owner || owner === TRON_ZERO_ADDRESS) continue;

      const resultStr = String(data.result ?? '0');
      if (resultStr === '0') continue; // slot sin mint

      const finishAt = Number(String(data.finish_at ?? '0'));
      const startAt  = Number(String(data.start_at  ?? '0'));

      rows.push({
        breed_result_id: resultStr,
        parent1_id:      String(data.parent1),
        parent2_id:      String(data.parent2),
        owner:           owner,  // base58 T…
        bred_at:         new Date((finishAt || startAt) * 1000).toISOString(),
        chain:           'tron',
      });
      valid++;
    }

    if (processed % 200 < TRON_CONCURRENCY) {
      console.log(
        `[Breeds] TRON: ${processed}/${maxId - startId + 1} procesados · ${valid} válidos`
      );
    }

    await sleep(TRON_DELAY_MS);
  }

  // Upsert
  if (rows.length > 0) {
    const CHUNK = 500;
    for (let i = 0; i < rows.length; i += CHUNK) {
      const chunk = rows.slice(i, i + CHUNK);
      const { error } = await supabase
        .from('all_breeds')
        .upsert(chunk, { onConflict: 'breed_result_id' });

      if (error) {
        console.error(`[Breeds] TRON upsert falló (${i}):`, error.message);
        throw error;
      }
      console.log(`[Breeds] TRON upsert OK ${i + chunk.length}/${rows.length}`);
    }
  }

  console.log(`[Breeds] ✅ TRON all_breeds · ${valid} breeds válidos`);

  // Marcar backfill como completado (solo la 1ª vez)
  if (!alreadyDone) {
    await supabase.from('indexer_state').upsert({
      key: 'tron_breed_backfill_done',
      last_block: 1,
      updated_at: new Date().toISOString(),
    });
    console.log('[Breeds] TRON: backfill marcado como completado');
  }
}

// ============================================================
// Main
// ============================================================
(async function main() {
  const started = Date.now();
  console.log(`[Breeds] Inicio ${new Date().toISOString()}`);

  try {
    // ── FASE 1: breeds de wallets conocidas (BSC) ───────────
    const wallets = await loadAllWallets();
    console.log(`[Breeds] Wallets a procesar: ${wallets.length}`);

    if (wallets.length > 0) {
      const breeding  = new ethers.Contract(BREEDING, BREEDING_ABI, provider);
      const multicall = new ethers.Contract(MULTICALL3, MULTICALL_ABI, provider);

      const ids = await fetchAllBreedIds(wallets, breeding, multicall);
      console.log(`[Breeds] breedIds únicos: ${ids.length}`);

      if (ids.length > 0) {
        const rows = await fetchBreedRows(ids, breeding, multicall);
        console.log(`[Breeds] Filas a upsertear en breedings: ${rows.length}`);
        await upsertBreedRows(rows);
      } else {
        console.log('[Breeds] Sin breeds on-chain para esas wallets');
      }
    } else {
      console.log('[Breeds] Sin wallets en user_wallets — saltando fase 1');
    }

    // ── FASE 2: BSC BreedFinish → all_breeds ────────────────
    try {
      await indexBscBreedFinish();
    } catch (e) {
      console.error('[Breeds] Fase BSC falló:', e.message);
    }

    // ── FASE 3: TRON breeds(0..N) → all_breeds ──────────────
    try {
      await indexTronBreeds();
    } catch (e) {
      console.error('[Breeds] Fase TRON falló:', e.message);
    }

    const elapsed = ((Date.now() - started) / 1000).toFixed(1);
    console.log(`[Breeds] ✅ Fin en ${elapsed}s`);
    process.exit(0);
  } catch (e) {
    console.error('[Breeds] ❌ FALLO:', e.message);
    console.error(e.stack);
    process.exit(1);
  }
})();