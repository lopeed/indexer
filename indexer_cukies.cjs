// indexer_cukies.cjs
// ============================================================
// Cukies Indexer — enumera todos los Cukies del contrato y los
// mina en la tabla `cukies` de Supabase. Usa Multicall3 para
// agrupar cientos de llamadas en una sola petición RPC.
//
// Ejecución recomendada: 1 vez al día (el totalSupply y los
// datos de cada Cukie cambian poco).
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
  console.error('[Cukies] Faltan variables de entorno');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

const RPC_URL = `https://bsc-mainnet.nodereal.io/v1/${BSCTRACE_API_KEY}`;
const provider = new ethers.JsonRpcProvider(RPC_URL, 56, { staticNetwork: true });

const CUKIE_NFT_CONTRACT = '0x0dbdebcc62f11005bf434abfad74564e896ac861';
const MULTICALL3 = '0xcA11bde05977b3631167028862bE2a173976CA11';

const NFT_ABI = [
  'function totalSupply() view returns (uint256)',
  'function tokenByIndex(uint256) view returns (uint256)',
  'function getCukie(uint256 tokenId) view returns (uint256 generation, uint8 skill1, uint8 skill2, uint8 skill3, uint8 skill4, uint8 skill5, uint8 skill6, uint8 energy, uint8 health)',
  'function ownerOf(uint256 tokenId) view returns (address)',
];

const MULTICALL_ABI = [
  'function aggregate3((address target, bool allowFailure, bytes callData)[] calls) payable returns ((bool success, bytes returnData)[])',
];

// ------------------------------------------------------------
// Chunks y rate limit
// ------------------------------------------------------------
const IDS_PER_CHUNK = 400;    // tokenByIndex por llamada multicall
const DATA_PER_CHUNK = 200;   // getCukie + ownerOf por llamada multicall
const CHUNK_DELAY_MS = 300;   // pausa entre chunks (rate limit suave)

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ------------------------------------------------------------
// Deriva la rareza del tokenId (fuente fiable)
//   1ª gen: primer dígito 1-6
//   2ª gen BSC: prefijo 101-106
// ------------------------------------------------------------
function extraerRareza(tokenId) {
  const s = String(tokenId);
  const first3 = Number(s.slice(0, 3));
  if (first3 >= 101 && first3 <= 106) return first3 - 100;
  const first1 = Number(s[0]);
  if (first1 >= 1 && first1 <= 6) return first1;
  return 1;
}

// ------------------------------------------------------------
// 1) Obtener todos los tokenIds con tokenByIndex vía Multicall3
// ------------------------------------------------------------
async function fetchAllTokenIds(total) {
  const nft = new ethers.Contract(CUKIE_NFT_CONTRACT, NFT_ABI, provider);
  const multicall = new ethers.Contract(MULTICALL3, MULTICALL_ABI, provider);

  const ids = [];
  const totalNum = Number(total);
  console.log(`[Cukies] totalSupply = ${totalNum}`);

  for (let start = 0; start < totalNum; start += IDS_PER_CHUNK) {
    const end = Math.min(start + IDS_PER_CHUNK, totalNum);
    const calls = [];
    for (let i = start; i < end; i++) {
      calls.push({
        target: CUKIE_NFT_CONTRACT,
        allowFailure: true,
        callData: nft.interface.encodeFunctionData('tokenByIndex', [i]),
      });
    }

    const results = await multicall.aggregate3.staticCall(calls);
    for (const r of results) {
      if (!r.success) continue;
      const id = nft.interface.decodeFunctionResult('tokenByIndex', r.returnData)[0];
      ids.push(id.toString());
    }

    console.log(`[Cukies] ${ids.length}/${totalNum} tokenIds obtenidos`);
    if (end < totalNum) await sleep(CHUNK_DELAY_MS);
  }

  return ids;
}

// ------------------------------------------------------------
// 2) Obtener datos (getCukie + ownerOf) en bloque vía Multicall3
// ------------------------------------------------------------
async function fetchAllData(tokenIds) {
  const nft = new ethers.Contract(CUKIE_NFT_CONTRACT, NFT_ABI, provider);
  const breedingInterface = new ethers.Interface(BREEDING_ABI);
  const multicall = new ethers.Contract(MULTICALL3, MULTICALL_ABI, provider);

  const rows = [];

  for (let start = 0; start < tokenIds.length; start += DATA_PER_CHUNK) {
    const end = Math.min(start + DATA_PER_CHUNK, tokenIds.length);
    const batch = tokenIds.slice(start, end);

    // 1. Llamadas getCukie
    const dataCalls = batch.map((id) => ({
      target: CUKIE_NFT_CONTRACT,
      allowFailure: true,
      callData: nft.interface.encodeFunctionData('getCukie', [id]),
    }));

    // 2. Llamadas ownerOf
    const ownerCalls = batch.map((id) => ({
      target: CUKIE_NFT_CONTRACT,
      allowFailure: true,
      callData: nft.interface.encodeFunctionData('ownerOf', [id]),
    }));

    // 3. Llamadas getNumBreedsByCukie (NUEVO)
    const breedCalls = batch.map((id) => ({
      target: BREEDING_CONTRACT,
      allowFailure: true,
      callData: breedingInterface.encodeFunctionData('getNumBreedsByCukie', [id]),
    }));

    // Ejecutamos las 3 en paralelo
    const [dataResults, ownerResults, breedResults] = await Promise.all([
      multicall.aggregate3.staticCall(dataCalls),
      multicall.aggregate3.staticCall(ownerCalls),
      multicall.aggregate3.staticCall(breedCalls),
    ]);

    for (let i = 0; i < batch.length; i++) {
      const id = batch[i];
      const dr = dataResults[i];
      const or = ownerResults[i];
      const br = breedResults[i];

      if (!dr.success) {
        console.warn(`[Cukies] getCukie fallo para ${id}, saltando`);
        continue;
      }

      const decoded = nft.interface.decodeFunctionResult('getCukie', dr.returnData);
      const generation = Number(decoded[0]);
      const skills = [
        Number(decoded[1]), Number(decoded[2]), Number(decoded[3]),
        Number(decoded[4]), Number(decoded[5]), Number(decoded[6]),
      ];
      const energy = Number(decoded[7]);
      const health = Number(decoded[8]);

      let owner = null;
      if (or.success) {
        owner = nft.interface.decodeFunctionResult('ownerOf', or.returnData)[0].toLowerCase();
      }

      let breedsDone = 0;
      if (br.success) {
        breedsDone = Number(breedingInterface.decodeFunctionResult('getNumBreedsByCukie', br.returnData)[0]);
      }

      rows.push({
        token_id: id,
        generation,
        rarity: extraerRareza(id),
        health,
        energy,
        skill1: skills[0], skill2: skills[1], skill3: skills[2],
        skill4: skills[3], skill5: skills[4], skill6: skills[5],
        owner,
        breeds_done: breedsDone, // <- NUEVO
        updated_at: new Date().toISOString(),
      });
    }
  }
  return rows;
}

// ------------------------------------------------------------
// 3) Upsert en Supabase en lotes
// ------------------------------------------------------------
async function upsertRows(rows) {
  const UPSERT_CHUNK = 500;
  for (let i = 0; i < rows.length; i += UPSERT_CHUNK) {
    const chunk = rows.slice(i, i + UPSERT_CHUNK);
    const { error } = await supabase
      .from('cukies')
      .upsert(chunk, { onConflict: 'token_id' });
    if (error) {
      console.error(`[Cukies] Error upsert lote ${i}:`, error.message);
      throw error;
    }
    console.log(`[Cukies] Upsert OK ${i + chunk.length}/${rows.length}`);
  }
}

// ------------------------------------------------------------
// Main
// ------------------------------------------------------------
(async function main() {
  const startedAt = Date.now();
  console.log(`[Cukies] Inicio ${new Date().toISOString()}`);

  try {
    const nft = new ethers.Contract(CUKIE_NFT_CONTRACT, NFT_ABI, provider);
    const total = await nft.totalSupply();
    if (!total || total === 0n) {
      console.warn('[Cukies] totalSupply = 0, nada que hacer');
      process.exit(0);
    }

    const ids = await fetchAllTokenIds(total);
    console.log(`[Cukies] IDs obtenidos: ${ids.length}`);

    const rows = await fetchAllData(ids);
    console.log(`[Cukies] Filas preparadas: ${rows.length}`);

    await upsertRows(rows);

    const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);
    console.log(`[Cukies] ✅ Fin en ${elapsed}s · ${rows.length} Cukies`);
    process.exit(0);
  } catch (e) {
    console.error('[Cukies] ❌ FALLO:', e.message);
    process.exit(1);
  }
})();