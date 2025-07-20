// src/do/FingerprintTracker.js (generateFingerprint 関数のみ修正)

export async function generateFingerprint(request) {
  const headers = request.headers;
  const cf = request.cf || {}; // request.cf オブジェクト

  let fingerprintString = "";

  // 1. User-Agent のコア部分 (揺れを吸収するために簡略化)
  const ua = headers.get("User-Agent") || "";
  const uaMatch = ua.match(/(Chrome)\/(\d+)\./i); // Chrome/120
  const osMatch = ua.match(/(Windows NT \d+\.\d+|Macintosh; Intel Mac OS X \d+_\d+_\d+|Linux)/i); // Windows NT 10.0, Mac OS X 10_15_7
  
  if (uaMatch && uaMatch[1] && uaMatch[2]) {
      fingerprintString += `UA:${uaMatch[1]}-${uaMatch[2]}`; // 例: UA:Chrome-120
  } else {
      fingerprintString += `UA:${ua}`; // マッチしない場合は全体を使う (フォールバック)
  }
  if (osMatch && osMatch[0]) {
      fingerprintString += `_OS:${osMatch[0].replace(/ /g, '_')}`; // 例: _OS:Macintosh;_Intel_Mac_OS_X_10_15_7
  }
  
  // 2. Accept ヘッダー群
  fingerprintString += `|AL:${headers.get("Accept-Language") || ""}`;
  fingerprintString += `|AE:${headers.get("Accept-Encoding") || ""}`;
  fingerprintString += `|A:${headers.get("Accept") || ""}`;

  // 3. Client Hints (存在すれば) - これらは非常に強力
  fingerprintString += `|SCU:${headers.get("Sec-Ch-Ua") || ""}`;
  fingerprintString += `|SCUM:${headers.get("Sec-Ch-Ua-Mobile") || ""}`;
  fingerprintString += `|SCUP:${headers.get("Sec-Ch-Ua-Platform") || ""}`; // typo修正済み

  // 4. Sec-Fetch ヘッダー群 - これらも強力
  fingerprintString += `|SFS:${headers.get("Sec-Fetch-Site") || ""}`;
  fingerprintString += `|SFM:${headers.get("Sec-Fetch-Mode") || ""}`;
  fingerprintString += `|SFD:${headers.get("Sec-Fetch-Dest") || ""}`;
  fingerprintString += `|SFU:${headers.get("Sec-Fetch-User") || ""}`;

  // 5. Referer (ある場合)
  fingerprintString += `|R:${headers.get("Referer") || ""}`;
  fingerprintString += `|UIR:${headers.get("Upgrade-Insecure-Requests") || ""}`;


  // 6. Cloudflare メタデータ (request.cf) - ネットワーク層の特性
  // ここで安定しているものだけを残す
  fingerprintString += `|ASN:${cf.asn || ""}`;       // AS番号 (安定している)
  fingerprintString += `|C:${cf.country || ""}`;   // 国コード (安定している)
  fingerprintString += `|TZ:${cf.timezone || ""}`;  // タイムゾーン (安定している)
  fingerprintString += `|COLO:${cf.colo || ""}`; // データセンターコード (安定している)
  fingerprintString += `|HP:${cf.httpProtocol || ""}`; // HTTPプロトコル (安定している)

  // ★★★ 修正: 常に揺れるTLS関連の項目を除外 ★★★
  // fingerprintString += `|TC:${cf.tlsCipher || ""}`; // TLS暗号スイート (今回は一致しているが、今後揺れる可能性)
  // fingerprintString += `|TV:${cf.tlsVersion || ""}`; // TLSバージョン (今回は一致しているが、今後揺れる可能性)
  // fingerprintString += `|TCHL:${cf.tlsClientHelloLength || ""}`; // 常に揺れる
  // fingerprintString += `|TCSR:${cf.tlsClientRandom || ""}`; // 常に揺れる
  // fingerprintString += `|TCE1:${cf.tlsClientExtensionsSha1 || ""}`; // 常に揺れる
  // fingerprintString += `|TCE1LE:${cf.tlsClientExtensionsSha1Le || ""}`; // 常に揺れる

  // ★★★ 修正: クライアント側のTCP/地理情報など、リクエストごと、位置ごとに変動する可能性のあるものを除外 ★★★
  // fingerprintString += cf.clientTcpRtt || "";
  // fingerprintString += cf.longitude || "";
  // fingerprintString += cf.latitude || "";
  // fingerprintString += cf.city || "";
  // fingerprintString += cf.region || "";
  // fingerprintString += cf.postalCode || "";


  // 7. IPアドレスのサブネットの一部 (安定性を損なうため、FPからは除外)
  // これらはBAD_BOT_IDの生成に使うのが適切
  /*
  const ip = headers.get("CF-Connecting-IP");
  if (ip) {
    if (ip.includes('.')) { // IPv4
      fingerprintString += `|IPS:${ip.split('.').slice(0, 3).join('.')}`;
    } else if (ip.includes(':')) { // IPv6
      fingerprintString += `|IPS6:${ip.split(':').slice(0, 4).join(':')}`;
    }
  }
  */

  // --- ハッシュ化 ---
  const encoder = new TextEncoder();
  const data = encoder.encode(fingerprintString);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8ToArray(hashBuffer));
  const fingerprint = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');

  return fingerprint;
}

// Durable Object内でparseLocaleが必要なため、ここに定義
// ユーティリティ関数は共有ファイルにまとめるのがベストだが、ここではFingerprintTracker.js内に含める
function parseLocale(path) {
  const trimmedPath = path.replace(/^\/+/, "").toLowerCase();
  const seg = trimmedPath.split("/")[0];

  // --- 特例：日本向けURL（トップ, /ja, /en） ---
  if (seg === "" || seg === "ja") {
    return { lang: "ja", country: "jp" };
  }
  if (seg === "en") {
    return { lang: "en", country: "jp" };
  }

  // --- 通常ロケール: xx-XX 形式 ---
  const match = seg.match(/^([a-z]{2})-([a-z]{2})$/i);
  if (match) {
    return { lang: match[1], country: match[2] };
  }

  // --- 不明ロケール ---
  return { lang: "unknown", country: "unknown" };
}
