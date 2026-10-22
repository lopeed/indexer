// indexer_breeds.cjs
// ============================================================
// Breeds Indexer — lee TODOS los breed records on-chain de
// todas las wallets conocidas y los upsertea en Supabase.
//
// Autoritativo: sobrevive a fallos del frontend capturando el
// breed_id (como el de la tx 0x73e7… con breedId 1222).
//
// Ejecución recomendada: cada 2-4h vía GitHub Actions.
// ============================================================

const { ethers } = require('ethers');
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const BSCTRACE_API_KEY = process.env.BSCTRACE_API_KEY;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY || !BSCTRACE_API_KEY) {
  console.error('[Breeds] Faltan variables de entorno');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

const RPC_URL = `https://bsc-mainnet.nodereal.io/v1/${BSCTRACE_API_KEY}`;
const provider = new ethers.JsonRpcProvider(RPC_URL, 56, { staticNetwork: true });

const BREEDING = '0x39be8c4fa342c5f3c10d7c16941a0946d29ade4a';
const MULTICALL3 = '0xcA11bde05977b3631167028862bE2a173976CA11';

const BREEDING_ABI = [
  'function getAllBreedsOwner(address _user) view returns (uint256[])',
  'function breeds(uint256) view returns (uint256 parent1, uint256 parent2, uint256 start_at, uint256 finish_at, address owner, bool completed, uint256 result)',
];

const MULTICALL_ABI = [
  'function aggregate3((address target, bool allowFailure, bytes callData)[] calls) payable returns ((bool success, bytes returnData)[])',
];

const WALLETS_PER_CHUNK = 100;   // wallets por multicall
const BREEDS_PER_CHUNK  = 150;   // breeds por multicall
const CHUNK_DELAY_MS    = 250;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ------------------------------------------------------------
// 1) Todas las wallets conocidas
// ------------------------------------------------------------
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

// ------------------------------------------------------------
// 2) getAllBreedsOwner por wallet (multicall)
// ------------------------------------------------------------
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
    console.log(`[Breeds] Wallets procesadas: ${Math.min(i + WALLETS_PER_CHUNK, wallets.length)}/${wallets.length} · IDs totales: ${allIds.size}`);
    if (i + WALLETS_PER_CHUNK < wallets.length) await sleep(CHUNK_DELAY_MS);
  }
  return [...allIds];
}

// ------------------------------------------------------------
// 3) breeds(id) para todos los IDs (multicall)
// ------------------------------------------------------------
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
          // NO incluimos start_tx_hash / breed_tx_hash aquí:
          // el upsert los respeta si ya existen.
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

// ------------------------------------------------------------
// 4) Upsert en Supabase
// ------------------------------------------------------------
async function upsertBreedRows(rows) {
  const CHUNK = 500;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    const { error } = await supabase
      .from('breedings')
      .upsert(chunk, { onConflict: 'breed_id' });
    if (error) {
      console.error(`[Breeds] upsert lote ${i} falló:`, error.message);
      throw error;
    }
    console.log(`[Breeds] Upsert OK ${i + chunk.length}/${rows.length}`);
  }
}

// ------------------------------------------------------------
// Main
// ------------------------------------------------------------
(async function main() {
  const started = Date.now();
  console.log(`[Breeds] Inicio ${new Date().toISOString()}`);

  try {
    const wallets = await loadAllWallets();
    console.log(`[Breeds] Wallets a procesar: ${wallets.length}`);
    if (wallets.length === 0) {
      console.log('[Breeds] Sin wallets en user_wallets — nada que hacer');
      process.exit(0);
    }

    const breeding = new ethers.Contract(BREEDING, BREEDING_ABI, provider);
    const multicall = new ethers.Contract(MULTICALL3, MULTICALL_ABI, provider);

    const ids = await fetchAllBreedIds(wallets, breeding, multicall);
    console.log(`[Breeds] breedIds únicos: ${ids.length}`);
    if (ids.length === 0) {
      console.log('[Breeds] Sin breeds on-chain');
      process.exit(0);
    }

    const rows = await fetchBreedRows(ids, breeding, multicall);
    console.log(`[Breeds] Filas a upsertear: ${rows.length}`);

    await upsertBreedRows(rows);

    const elapsed = ((Date.now() - started) / 1000).toFixed(1);
    console.log(`[Breeds] ✅ Fin en ${elapsed}s · ${rows.length} breeds sincronizadas`);
    process.exit(0);
  } catch (e) {
    console.error('[Breeds] ❌ FALLO:', e.message);
    process.exit(1);
  }
})();