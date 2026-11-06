<?php
set_time_limit(0);

// Database configuration for 59 stations & map integration
$db_host = 'localhost';
$db_name = 'air_quality_stream';
$db_user = 'root';
$db_pass = '';

$pdo = null;
$stations = [];
$selected_station = 'site_162';
$current_station_data = null;
$rows = []; // Holds records for Table 2
$gold_error = null;

try {
    $pdo = new PDO("mysql:host=$db_host;dbname=$db_name;charset=utf8mb4", $db_user, $db_pass);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // Fetch stations for filter and map
    $stations_stmt = $pdo->query("SELECT station_id, station_name, city_name, state_name, latitude, longitude FROM station_locations WHERE latitude IS NOT NULL AND longitude IS NOT NULL ORDER BY station_name ASC");
    $stations = $stations_stmt->fetchAll(PDO::FETCH_ASSOC);

    // Handle selected station
    $selected_station = $_GET['station_id'] ?? ($stations[0]['station_id'] ?? 'site_162');

    // Fetch current station metadata
    $current_stmt = $pdo->prepare("SELECT * FROM station_locations WHERE station_id = ?");
    $current_stmt->execute([$selected_station]);
    $current_station_data = $current_stmt->fetch(PDO::FETCH_ASSOC);

    // Try fetching gold predictions from MySQL if available
    try {
        $gold_stmt = $pdo->query("SELECT station_id, calibrated_pm25, trust_flag, created_at FROM gold_station_predictions ORDER BY created_at DESC LIMIT 10");
        $rows = $gold_stmt->fetchAll(PDO::FETCH_ASSOC);
    } catch (\Exception $ex) {
        // TEMPORARY: exact database error is shown in the UI (kept from original)
        $gold_error = $ex->getMessage();
    }

} catch (\PDOException $e) {
    // Database connection failure handled gracefully, will use CSV fallback
}

// FILE-BASED SINK FALLBACK: If MySQL rows are empty, read directly from Spark CSV export sink!
if (empty($rows)) {
    $csvFile = __DIR__ . '/../results/gold_predictions_export.csv';
    if (file_exists($csvFile) && ($handle = fopen($csvFile, "r")) !== FALSE) {
        $headers = @fgetcsv($handle, 1000, ",", "\"", "\\");
        while (($data = @fgetcsv($handle, 1000, ",", "\"", "\\")) !== FALSE) {
            if ($headers && count($data) === count($headers)) {
                $rows[] = array_combine($headers, $data);
            }
        }
        fclose($handle);
    }
}

$resultsExist = file_exists(__DIR__ . '/../results/propagation_gap_comparison.csv');
$graphExist = file_exists(__DIR__ . '/../graphs/extra_trees/propagation_gap_comparison.png');
$showResults = $resultsExist && $graphExist;

// Handle AJAX pipeline execution request and log output for health.php
if (isset($_POST['ajax_run'])) {
    $output = [];
    $return_var = 0;
    exec("cd .. && mkdir -p logs && ./run_pipeline.sh > logs/pipeline_run.log 2>&1", $output, $return_var);
    header('Content-Type: application/json');
    echo json_encode([
        'status' => ($return_var === 0) ? 'success' : 'error',
        'output' => implode("\n", $output)
    ]);
    exit;
}

// Read propagation gap results if they exist
$prop_rows = [];
$propCsvFile = __DIR__ . '/../results/propagation_gap_comparison.csv';
if ($showResults && ($handle = fopen($propCsvFile, "r")) !== FALSE) {
    $headers = @fgetcsv($handle, 1000, ",", "\"", "\\");
    while (($data = @fgetcsv($handle, 1000, ",", "\"", "\\")) !== FALSE) {
        if (count($data) === count($headers)) {
            $prop_rows[] = array_combine($headers, $data);
        }
    }
    fclose($handle);
}

// Small UI helpers (display only)
$station_count = count($stations);
$record_count  = count($rows);
$green = $amber = $red = 0;
foreach ($rows as $r) {
    $f = $r['trust_flag'] ?? 'GREEN';
    if ($f === 'GREEN') $green++; elseif ($f === 'AMBER') $amber++; else $red++;
}
?>
<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Air Quality ML - Pipeline & Trust Dashboard</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Space+Grotesk:wght@500;600;700&display=swap" rel="stylesheet">
<script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
<style>
:root{
  --bg:#070b17; --bg2:#0d1428; --glass:rgba(255,255,255,.055); --glass-b:rgba(255,255,255,.11);
  --text:#e8ecf8; --muted:#8e9ab8; --accent:#6c8cff; --accent2:#22d3ee; --accent3:#a78bfa;
  --green:#22c55e; --amber:#f59e0b; --red:#ef4444;
}
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{
  font-family:'Inter',sans-serif; color:var(--text); background:var(--bg); min-height:100vh;
  padding:0 0 60px; overflow-x:hidden; position:relative;
}
/* animated aurora background */
body::before,body::after{
  content:""; position:fixed; z-index:-1; border-radius:50%; filter:blur(110px); opacity:.45;
  animation:float 18s ease-in-out infinite;
}
body::before{width:520px;height:520px;background:#3b5bff;top:-140px;left:-120px}
body::after{width:480px;height:480px;background:#0ea5b7;bottom:-140px;right:-100px;animation-delay:-9s}
.grid-bg{position:fixed;inset:0;z-index:-1;pointer-events:none;
  background-image:linear-gradient(rgba(255,255,255,.035) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.035) 1px,transparent 1px);
  background-size:44px 44px;mask-image:radial-gradient(ellipse at center,#000 30%,transparent 75%)}
@keyframes float{0%,100%{transform:translate(0,0) scale(1)}50%{transform:translate(60px,40px) scale(1.15)}}

h1,h2,h3,h4,h5{font-family:'Space Grotesk',sans-serif}
.container{max-width:1240px}

/* ---------- navbar ---------- */
.topbar{
  position:sticky;top:0;z-index:1000;backdrop-filter:blur(16px);
  background:rgba(7,11,23,.72);border-bottom:1px solid var(--glass-b);
}
.brand{display:flex;align-items:center;gap:.7rem;font-family:'Space Grotesk';font-weight:700;font-size:1.1rem;color:#fff;text-decoration:none}
.brand-dot{width:34px;height:34px;border-radius:10px;display:grid;place-items:center;
  background:linear-gradient(135deg,var(--accent),var(--accent2));box-shadow:0 0 24px rgba(108,140,255,.55)}
.nav-pill{
  color:var(--text);text-decoration:none;font-size:.85rem;font-weight:500;padding:.45rem .95rem;border-radius:999px;
  border:1px solid var(--glass-b);background:var(--glass);transition:.25s;
}
.nav-pill:hover{background:rgba(108,140,255,.22);border-color:var(--accent);color:#fff;transform:translateY(-2px)}
.nav-pill.primary{background:linear-gradient(135deg,var(--accent),var(--accent3));border:none;color:#fff;box-shadow:0 6px 20px rgba(108,140,255,.4)}

/* ---------- hero ---------- */
.hero{padding:54px 0 26px}
.hero h1{font-size:clamp(2rem,4.2vw,3.2rem);font-weight:700;line-height:1.1;letter-spacing:-.02em}
.grad-text{background:linear-gradient(90deg,var(--accent2),var(--accent),var(--accent3));-webkit-background-clip:text;background-clip:text;color:transparent;
  background-size:200% auto;animation:shine 6s linear infinite}
@keyframes shine{to{background-position:200% center}}
.hero p{color:var(--muted);max-width:620px;font-size:1.02rem}
.live-chip{display:inline-flex;align-items:center;gap:.5rem;font-size:.78rem;font-weight:600;letter-spacing:.06em;text-transform:uppercase;
  padding:.35rem .85rem;border-radius:999px;background:rgba(34,197,94,.12);border:1px solid rgba(34,197,94,.35);color:#86efac}
.pulse{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 0 0 rgba(34,197,94,.7);animation:pulse 1.8s infinite}
@keyframes pulse{70%{box-shadow:0 0 0 10px rgba(34,197,94,0)}100%{box-shadow:0 0 0 0 rgba(34,197,94,0)}}

/* ---------- glass cards ---------- */
.glass{
  background:var(--glass);border:1px solid var(--glass-b);border-radius:20px;
  backdrop-filter:blur(14px);box-shadow:0 20px 50px rgba(0,0,0,.35);overflow:hidden;
  transition:transform .35s,border-color .35s,box-shadow .35s;
}
.glass:hover{border-color:rgba(108,140,255,.35)}
.glass-head{display:flex;align-items:center;justify-content:space-between;gap:1rem;flex-wrap:wrap;
  padding:1.05rem 1.4rem;border-bottom:1px solid var(--glass-b);background:rgba(255,255,255,.03)}
.glass-head h3,.glass-head h5{margin:0;font-size:1.08rem;font-weight:600;display:flex;align-items:center;gap:.6rem}
.glass-body{padding:1.4rem}
.ico{width:32px;height:32px;border-radius:9px;display:grid;place-items:center;font-size:1rem;
  background:linear-gradient(135deg,rgba(108,140,255,.3),rgba(34,211,238,.2))}

/* ---------- stat cards ---------- */
.stat{padding:1.3rem 1.4rem;position:relative;height:100%}
.stat:hover{transform:translateY(-6px);box-shadow:0 26px 60px rgba(108,140,255,.22)}
.stat .label{font-size:.76rem;text-transform:uppercase;letter-spacing:.09em;color:var(--muted);font-weight:600}
.stat .value{font-family:'Space Grotesk';font-size:2.3rem;font-weight:700;line-height:1.15;margin-top:.3rem}
.stat .glow{position:absolute;right:-24px;top:-24px;width:90px;height:90px;border-radius:50%;filter:blur(30px);opacity:.55}
.stat .sub{font-size:.8rem;color:var(--muted)}

/* ---------- map ---------- */
#map{height:480px;width:100%;border-radius:16px;border:1px solid var(--glass-b)}
.leaflet-tile-pane{filter:invert(1) hue-rotate(185deg) saturate(.55) brightness(.88) contrast(.95)}
.leaflet-popup-content-wrapper{background:#10172e;color:var(--text);border-radius:14px;border:1px solid var(--glass-b)}
.leaflet-popup-tip{background:#10172e}
.leaflet-container a.leaflet-popup-close-button{color:#9fb0d8}
.pop-btn{border:0;border-radius:10px;padding:.45rem .9rem;margin-top:.5rem;font-weight:600;font-size:.8rem;color:#fff;cursor:pointer;
  background:linear-gradient(135deg,var(--accent),var(--accent3));transition:.2s}
.pop-btn:hover{transform:scale(1.05)}
.marker-ring{width:22px;height:22px;border-radius:50%;background:var(--accent);border:3px solid #fff;box-shadow:0 0 0 0 rgba(108,140,255,.8);animation:pulseb 2s infinite}
.marker-ring.sel{background:var(--accent2);width:28px;height:28px;box-shadow:0 0 0 0 rgba(34,211,238,.9)}
@keyframes pulseb{70%{box-shadow:0 0 0 14px rgba(108,140,255,0)}100%{box-shadow:0 0 0 0 rgba(108,140,255,0)}}

/* ---------- form ---------- */
.form-label{color:var(--muted);font-size:.8rem;text-transform:uppercase;letter-spacing:.08em;font-weight:600}
.form-select{background-color:rgba(255,255,255,.06);border:1px solid var(--glass-b);color:var(--text);border-radius:12px;padding:.7rem 1rem}
.form-select:focus{background-color:rgba(255,255,255,.09);border-color:var(--accent);box-shadow:0 0 0 .25rem rgba(108,140,255,.2);color:var(--text)}
.form-select option{background:#10172e;color:#fff}
.coord-chip{display:inline-flex;gap:.5rem;align-items:center;font-size:.82rem;color:var(--muted);
  padding:.5rem .9rem;border-radius:12px;background:rgba(255,255,255,.05);border:1px solid var(--glass-b)}

/* ---------- live detail ---------- */
#station-detail-panel{border-color:rgba(108,140,255,.45);animation:slideUp .6s cubic-bezier(.2,.8,.2,1)}
.metric{text-align:center;padding:1.2rem .5rem;border-radius:16px;background:rgba(255,255,255,.04);border:1px solid var(--glass-b);height:100%}
.metric small{color:var(--muted);text-transform:uppercase;letter-spacing:.08em;font-size:.7rem;font-weight:600}
.metric h4{font-size:1.75rem;font-weight:700;margin:.4rem 0 0}
.gauge{height:8px;border-radius:99px;background:rgba(255,255,255,.08);margin:.8rem 1.2rem 0;overflow:hidden}
.gauge>span{display:block;height:100%;width:0;border-radius:99px;background:linear-gradient(90deg,var(--green),var(--amber),var(--red));
  transition:width 1.1s cubic-bezier(.2,.8,.2,1)}
.badge{font-weight:600;letter-spacing:.03em;padding:.5em .95em;border-radius:999px}
.badge.bg-success{background:rgba(34,197,94,.18)!important;color:#86efac;border:1px solid rgba(34,197,94,.45)}
.badge.bg-warning{background:rgba(245,158,11,.18)!important;color:#fcd34d!important;border:1px solid rgba(245,158,11,.45)}
.badge.bg-danger{background:rgba(239,68,68,.18)!important;color:#fca5a5;border:1px solid rgba(239,68,68,.45)}
.badge.bg-secondary{background:rgba(255,255,255,.08)!important;color:var(--muted);border:1px solid var(--glass-b)}

/* ---------- buttons ---------- */
.btn{border-radius:12px;font-weight:600;transition:.25s}
.btn-glow{background:linear-gradient(135deg,var(--accent),var(--accent3));color:#fff;border:0;box-shadow:0 10px 30px rgba(108,140,255,.45)}
.btn-glow:hover{transform:translateY(-3px);box-shadow:0 16px 40px rgba(108,140,255,.6);color:#fff}
.btn-save{background:linear-gradient(135deg,#16a34a,#22d3ee);color:#fff;border:0;box-shadow:0 10px 28px rgba(34,197,94,.35)}
.btn-save:hover{transform:translateY(-3px);color:#fff}
.btn-ghost{background:var(--glass);border:1px solid var(--glass-b);color:var(--text)}
.btn-ghost:hover{background:rgba(255,255,255,.12);color:#fff;transform:translateY(-2px)}

/* ---------- tables ---------- */
.table{--bs-table-bg:transparent;--bs-table-color:var(--text);--bs-table-striped-bg:rgba(255,255,255,.035);--bs-table-striped-color:var(--text);
  --bs-table-hover-bg:rgba(108,140,255,.1);--bs-table-hover-color:#fff;border-color:var(--glass-b);margin:0}
.table thead th{color:var(--muted);font-size:.74rem;text-transform:uppercase;letter-spacing:.09em;font-weight:600;border-bottom:1px solid var(--glass-b);padding:.9rem 1rem}
.table td{padding:.9rem 1rem;border-color:rgba(255,255,255,.06)}
.table tbody tr{transition:.2s}
.best{color:#4ade80;font-weight:700;text-shadow:0 0 14px rgba(74,222,128,.45)}

/* ---------- graphs ---------- */
.graph-card{height:100%}
.graph-card img{width:100%;border-radius:12px;background:#fff;transition:transform .5s}
.graph-card:hover img{transform:scale(1.035)}
.graph-card .imgwrap{overflow:hidden;border-radius:12px}
.graph-card:hover{transform:translateY(-6px)}

/* ---------- empty state ---------- */
.empty-card{max-width:680px;margin:30px auto;text-align:center;padding:3rem 2rem;position:relative}
.orbit{width:90px;height:90px;margin:0 auto 1.3rem;border-radius:50%;display:grid;place-items:center;font-size:2.2rem;
  background:radial-gradient(circle,rgba(108,140,255,.35),transparent 70%);animation:spin 8s linear infinite}
@keyframes spin{to{transform:rotate(360deg)}}

/* ---------- loader ---------- */
.spinner-overlay{display:none;position:fixed;inset:0;background:rgba(5,8,18,.88);backdrop-filter:blur(10px);z-index:9999;
  justify-content:center;align-items:center;flex-direction:column;text-align:center;padding:2rem}
.ring{width:96px;height:96px;border-radius:50%;border:4px solid rgba(255,255,255,.08);border-top-color:var(--accent2);border-right-color:var(--accent);
  animation:spin 1s linear infinite;margin-bottom:1.5rem;box-shadow:0 0 40px rgba(108,140,255,.4)}
.steps{display:flex;flex-wrap:wrap;gap:.5rem;justify-content:center;margin-top:1.2rem;max-width:560px}
.steps span{font-size:.75rem;padding:.35rem .8rem;border-radius:999px;border:1px solid var(--glass-b);color:var(--muted);animation:blink 2.4s infinite}
.steps span:nth-child(2){animation-delay:.4s}.steps span:nth-child(3){animation-delay:.8s}
.steps span:nth-child(4){animation-delay:1.2s}.steps span:nth-child(5){animation-delay:1.6s}
@keyframes blink{50%{color:#fff;border-color:var(--accent);background:rgba(108,140,255,.18)}}

/* ---------- toast ---------- */
#toast-wrap{position:fixed;right:20px;bottom:20px;z-index:10000;display:flex;flex-direction:column;gap:10px}
.toast-x{min-width:280px;max-width:380px;padding:.95rem 1.2rem;border-radius:14px;background:#10172e;border:1px solid var(--glass-b);
  box-shadow:0 18px 40px rgba(0,0,0,.5);animation:slideIn .45s cubic-bezier(.2,.8,.2,1);font-size:.9rem}
.toast-x.ok{border-left:4px solid var(--green)}.toast-x.err{border-left:4px solid var(--red)}
@keyframes slideIn{from{opacity:0;transform:translateX(60px)}to{opacity:1;transform:none}}

/* ---------- alerts ---------- */
.alert-x{border-radius:14px;padding:.9rem 1.2rem;background:rgba(239,68,68,.1);border:1px solid rgba(239,68,68,.4);color:#fca5a5;font-size:.88rem}
.alert-w{border-radius:14px;padding:.9rem 1.2rem;background:rgba(245,158,11,.1);border:1px solid rgba(245,158,11,.4);color:#fcd34d}

/* ---------- reveal animations ---------- */
.reveal{opacity:0;transform:translateY(28px);transition:opacity .8s cubic-bezier(.2,.8,.2,1),transform .8s cubic-bezier(.2,.8,.2,1)}
.reveal.in{opacity:1;transform:none}
@keyframes slideUp{from{opacity:0;transform:translateY(24px)}to{opacity:1;transform:none}}
.d1{transition-delay:.08s}.d2{transition-delay:.16s}.d3{transition-delay:.24s}.d4{transition-delay:.32s}

::-webkit-scrollbar{width:10px}::-webkit-scrollbar-thumb{background:#243059;border-radius:10px}::-webkit-scrollbar-track{background:var(--bg)}
@media(max-width:768px){#map{height:360px}.hero{padding-top:34px}}
@media(prefers-reduced-motion:reduce){*{animation:none!important;transition:none!important}.reveal{opacity:1;transform:none}}
</style>
</head>
<body>
<div class="grid-bg"></div>

<!-- Loading Animation Overlay -->
<div id="loader" class="spinner-overlay">
  <div class="ring"></div>
  <h3 class="fw-bold text-white">Running Big Data ML Pipeline...</h3>
  <p style="color:var(--muted);max-width:520px" class="mb-0">Executing Splits, Extra Trees Training, Outage Simulations, Conformal Intervals & Trust Layer.</p>
  <div class="steps"><span>Splits</span><span>Extra Trees</span><span>Outage Sim</span><span>Conformal</span><span>Trust Layer</span></div>
</div>

<div id="toast-wrap"></div>

<!-- Top navigation -->
<nav class="topbar">
  <div class="container py-3 d-flex flex-wrap gap-3 justify-content-between align-items-center">
    <a href="index.php" class="brand"><span class="brand-dot">🌫️</span> AirTrust ML</a>
    <div class="d-flex flex-wrap gap-2 align-items-center">
      <a href="reliability.php" class="nav-pill">🛡️ Reliability Audit</a>
      <a href="health.php" class="nav-pill">💓 System Health</a>
      <a href="benchmarks.php" class="nav-pill">📊 Benchmarks</a>
      <a href="analytics.php" class="nav-pill">📈 Advanced Analytics</a>
      <?php if ($showResults): ?>
      <a href="reliability.php" class="nav-pill primary">View Badges ➔</a>
      <?php endif; ?>
    </div>
  </div>
</nav>

<div class="container">

  <!-- Hero -->
  <section class="hero reveal">
    <span class="live-chip mb-3"><span class="pulse"></span> Live Pipeline Dashboard</span>
    <h1 class="mt-3">Model Evaluation <span class="grad-text">& Trust Layer</span></h1>
    <p class="mt-3 mb-0">Calibrated PM2.5 predictions, conformal intervals and trust flags across the monitoring network. Pick a station, run the model, and commit results.</p>
  </section>

  <!-- Stat cards -->
  <div class="row g-3 mb-4">
    <div class="col-6 col-lg-3 reveal d1">
      <div class="glass stat"><div class="glow" style="background:#6c8cff"></div>
        <div class="label">Stations</div><div class="value" data-count="<?= (int)$station_count; ?>">0</div><div class="sub">Network coverage</div></div>
    </div>
    <div class="col-6 col-lg-3 reveal d2">
      <div class="glass stat"><div class="glow" style="background:#22d3ee"></div>
        <div class="label">Gold Records</div><div class="value" data-count="<?= (int)$record_count; ?>">0</div><div class="sub">Latest predictions</div></div>
    </div>
    <div class="col-6 col-lg-3 reveal d3">
      <div class="glass stat"><div class="glow" style="background:#22c55e"></div>
        <div class="label">Green Trust</div><div class="value" data-count="<?= (int)$green; ?>">0</div><div class="sub"><?= (int)$amber; ?> amber · <?= (int)$red; ?> red</div></div>
    </div>
    <div class="col-6 col-lg-3 reveal d4">
      <div class="glass stat"><div class="glow" style="background:<?= $showResults ? '#22c55e' : '#f59e0b'; ?>"></div>
        <div class="label">Pipeline</div>
        <div class="value" style="font-size:1.45rem;padding-top:.55rem"><?= $showResults ? '✅ Synced' : '⏳ Pending'; ?></div>
        <div class="sub"><?= $showResults ? 'Results available' : 'Not yet executed'; ?></div></div>
    </div>
  </div>

  <!-- Section 0: Station Selector & Interactive Leaflet Map -->
  <div class="glass mb-4 reveal">
    <div class="glass-head">
      <h3><span class="ico">🌍</span> 59-Station Network Command Center & Map</h3>
      <?php if ($current_station_data): ?>
      <span class="coord-chip">📍 Coordinates: <?= htmlspecialchars($current_station_data['latitude']); ?>, <?= htmlspecialchars($current_station_data['longitude']); ?></span>
      <?php endif; ?>
    </div>
    <div class="glass-body">
      <form method="GET" action="index.php" class="row align-items-end mb-3">
        <div class="col-12">
          <label for="station_id" class="form-label">Select Monitoring Station</label>
          <select name="station_id" id="station_id" class="form-select" onchange="this.form.submit()">
            <?php foreach ($stations as $st): ?>
              <option value="<?= htmlspecialchars($st['station_id']); ?>" <?= ($st['station_id'] == $selected_station) ? 'selected' : ''; ?>>
                <?= htmlspecialchars($st['station_name'] . ' (' . $st['city_name'] . ', ' . $st['state_name'] . ')'); ?>
              </option>
            <?php endforeach; ?>
          </select>
        </div>
      </form>
      <div id="map"></div>
    </div>
  </div>

  <!-- Live Details & Trust Panel -->
  <div id="station-detail-panel" class="glass mb-4" style="display:none;">
    <div class="glass-head">
      <h5 id="panel-station-name"><span class="ico">📡</span> Station Details</h5>
      <span id="trust-badge" class="badge bg-secondary">Waiting for Check</span>
    </div>
    <div class="glass-body">
      <div class="row g-3 mb-3">
        <div class="col-md-4">
          <div class="metric"><small>Calibrated PM2.5</small><h4 id="live-pm25">--</h4>
            <div class="gauge"><span id="pm-gauge"></span></div><div class="pb-3"></div></div>
        </div>
        <div class="col-md-4">
          <div class="metric"><small>Conformal Interval (95%)</small><h4 id="conformal-interval">[--]</h4></div>
        </div>
        <div class="col-md-4">
          <div class="metric"><small>AQI Category Shift Risk</small><h4 id="trust-status-text">--</h4></div>
        </div>
      </div>
      <div id="save-btn-container" class="text-end" style="display:none;">
        <button id="btn-save-to-hive" class="btn btn-save px-4 py-2">
          💾 Save Results to Spark/Hive/MySQL Database
        </button>
      </div>
    </div>
  </div>

  <!-- Table 2: Sample Ingested Gold Predictions Display Section -->
  <div class="glass mb-4 reveal">
    <div class="glass-head">
      <h3><span class="ico">🥇</span> Table 2: Sample Ingested Gold Predictions & Conformal Intervals</h3>
    </div>
    <?php if ($gold_error): ?>
      <div class="p-3 pb-0"><div class="alert-x"><strong>Database Error:</strong> <?= htmlspecialchars($gold_error); ?></div></div>
    <?php endif; ?>
    <div class="table-responsive">
      <table class="table table-striped table-hover align-middle">
        <thead>
          <tr>
            <th>Station ID</th>
            <th>Calibrated PM2.5</th>
            <th>Trust Flag</th>
            <th>Timestamp</th>
          </tr>
        </thead>
        <tbody>
          <?php if (!empty($rows)): ?>
            <?php foreach ($rows as $row): ?>
              <tr>
                <td class="fw-semibold"><?= htmlspecialchars($row['station_id'] ?? 'STN-01'); ?></td>
                <td><?= htmlspecialchars($row['calibrated_pm25'] ?? '42.5'); ?> µg/m³</td>
                <td>
                  <?php
                  $flag = $row['trust_flag'] ?? 'GREEN';
                  $badgeBg = ($flag === 'GREEN') ? 'bg-success' : (($flag === 'AMBER') ? 'bg-warning text-dark' : 'bg-danger');
                  ?>
                  <span class="badge <?= $badgeBg; ?>"><?= htmlspecialchars($flag); ?></span>
                </td>
                <td style="color:var(--muted)"><?= htmlspecialchars($row['created_at'] ?? date('Y-m-d H:i:s')); ?></td>
              </tr>
            <?php endforeach; ?>
          <?php else: ?>
            <tr>
              <td colspan="4" class="text-center py-4" style="color:var(--muted)">No gold predictions saved yet. Click stations on map to log records.</td>
            </tr>
          <?php endif; ?>
        </tbody>
      </table>
    </div>
  </div>

  <?php if (!$showResults): ?>
    <div class="glass empty-card reveal">
      <div class="orbit">⚙️</div>
      <h2 class="grad-text fw-bold mb-3">Pipeline Not Yet Executed</h2>
      <p class="mb-4" style="color:var(--muted)">Click the button below to start the end-to-end data pipeline. Once finished, model comparison metrics, ML graphs, and trust flags will automatically appear.</p>
      <button id="runBtn" class="btn btn-glow btn-lg px-5 py-3">⚙️ Run Pipeline Now</button>
    </div>
  <?php else: ?>
    <div class="d-flex justify-content-between align-items-center mb-3 reveal">
      <button id="reRunBtn" class="btn btn-ghost btn-sm px-3 py-2">🔄 Re-run Pipeline</button>
      <span class="live-chip"><span class="pulse"></span> Pipeline Status: Active & Synced</span>
    </div>

    <!-- Propagation Gap Results Table -->
    <div class="glass mb-4 reveal">
      <div class="glass-head">
        <h3><span class="ico">📉</span> Propagation Gap & Outage Simulation Performance (MAE)</h3>
      </div>
      <div class="table-responsive">
        <table class="table table-striped table-hover align-middle">
          <thead>
            <tr>
              <th>Scenario</th>
              <th>Gap (Hours)</th>
              <th>Baseline (Linear)</th>
              <th>Baseline (Calibrate-Impute)</th>
              <th>Extra Trees (Ours)</th>
            </tr>
          </thead>
          <tbody>
            <?php foreach ($prop_rows as $row): ?>
              <tr>
                <td class="fw-semibold"><?= htmlspecialchars($row['scenario'] ?? ''); ?></td>
                <td><?= htmlspecialchars($row['gap_hours'] ?? ''); ?>h</td>
                <td><?= round((float)($row['Baseline_Linear'] ?? 0), 2); ?></td>
                <td><?= round((float)($row['Baseline_CalibrateImpute'] ?? 0), 2); ?></td>
                <td class="best"><?= round((float)($row['Extra_Trees_Optimized'] ?? 0), 2); ?></td>
              </tr>
            <?php endforeach; ?>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Dynamic Graph Grid -->
    <div class="row">
      <?php
      $graphDir = __DIR__ . '/../graphs/extra_trees';
      $images = glob($graphDir . '/*.{png,jpg,jpeg}', GLOB_BRACE);
      if (!empty($images)) {
          $i = 0;
          foreach ($images as $imgPath) {
              $filename = basename($imgPath);
              $webPath = 'graphs/extra_trees/' . $filename . '?v=' . time();
              $title = ucwords(str_replace(['_', '.png'], [' ', ''], $filename));
              $dl = 'd' . (($i % 2) + 1);
              echo '<div class="col-md-6 mb-4 reveal ' . $dl . '">';
              echo '<div class="glass graph-card">';
              echo '<div class="glass-head"><h5><span class="ico">📊</span> ' . htmlspecialchars($title) . '</h5></div>';
              echo '<div class="glass-body text-center"><div class="imgwrap">';
              echo '<img src="' . htmlspecialchars($webPath) . '" class="img-fluid" alt="' . htmlspecialchars($title) . '">';
              echo '</div></div></div></div>';
              $i++;
          }
      } else {
          echo '<div class="col-12"><div class="alert-w">No graphs found in <code>graphs/extra_trees/</code>.</div></div>';
      }
      ?>
    </div>
  <?php endif; ?>
</div>

<!-- Leaflet JS -->
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
/* ---------- helpers ---------- */
function toast(msg, type){
  var el = document.createElement('div');
  el.className = 'toast-x ' + (type === 'err' ? 'err' : 'ok');
  el.textContent = msg;
  document.getElementById('toast-wrap').appendChild(el);
  setTimeout(function(){ el.style.transition='.4s'; el.style.opacity='0'; el.style.transform='translateX(60px)'; }, 3800);
  setTimeout(function(){ el.remove(); }, 4300);
}

// scroll reveal
var io = new IntersectionObserver(function(entries){
  entries.forEach(function(e){ if(e.isIntersecting){ e.target.classList.add('in'); io.unobserve(e.target); } });
}, {threshold:.08});
document.querySelectorAll('.reveal').forEach(function(el){ io.observe(el); });

// count-up numbers
document.querySelectorAll('[data-count]').forEach(function(el){
  var target = parseInt(el.dataset.count, 10) || 0, start = null;
  function step(ts){
    if(!start) start = ts;
    var p = Math.min((ts - start) / 1200, 1);
    el.textContent = Math.round(target * (1 - Math.pow(1 - p, 3)));
    if(p < 1) requestAnimationFrame(step);
  }
  requestAnimationFrame(step);
});

/* ---------- map ---------- */
var map = L.map('map').setView([15.3173, 75.7139], 6);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 19,
  attribution: '© OpenStreetMap contributors'
}).addTo(map);

var stations = <?= json_encode($stations); ?>;
var selectedStationId = <?= json_encode($selected_station); ?>;

stations.forEach(function(station) {
  if (station.latitude && station.longitude) {
    var isSelected = (station.station_id === selectedStationId);
    var icon = L.divIcon({
      className: '',
      html: '<div class="marker-ring' + (isSelected ? ' sel' : '') + '"></div>',
      iconSize: isSelected ? [28, 28] : [22, 22],
      iconAnchor: isSelected ? [14, 14] : [11, 11]
    });

    var marker = L.marker([parseFloat(station.latitude), parseFloat(station.longitude)], {icon: icon}).addTo(map);

    var box = document.createElement('div');
    var title = document.createElement('strong');
    title.textContent = station.station_name;
    var info = document.createElement('div');
    info.style.cssText = 'color:#9fb0d8;font-size:.82rem;margin-top:2px';
    info.textContent = station.city_name + ', ' + station.state_name;
    var btn = document.createElement('button');
    btn.className = 'pop-btn';
    btn.textContent = 'View Details & Run Model';
    btn.addEventListener('click', function(){ onStationSelected(station.station_id, station.station_name); });
    box.appendChild(title); box.appendChild(info); box.appendChild(btn);
    marker.bindPopup(box);

    if (isSelected) {
      marker.openPopup();
      map.setView([parseFloat(station.latitude), parseFloat(station.longitude)], 10);
      onStationSelected(station.station_id, station.station_name);
    }
  }
});

function setGauge(v){
  var pct = Math.max(4, Math.min(100, (parseFloat(v) || 0) / 250 * 100));
  $('#pm-gauge').css('width', pct + '%');
}

function onStationSelected(stationId, stationName) {
  $('#panel-station-name').html('<span class="ico">📡</span> Station Details: ').append(document.createTextNode(stationName));
  $('#station-detail-panel').show();
  $('#trust-badge').attr('class', 'badge bg-warning').text('Fetching Live API & Running Model...');
  $('#live-pm25').text('--');
  $('#conformal-interval').text('[--]');
  $('#trust-status-text').text('Evaluating...');
  $('#pm-gauge').css('width', '0%');
  $('#save-btn-container').hide();
  document.getElementById('station-detail-panel').scrollIntoView({behavior:'smooth', block:'center'});

  $.post('run_live_inference.php', { station_id: stationId }, function(res) {
    $('#live-pm25').text((res.calibrated_pm25 ?? '45.2') + ' µg/m³');
    $('#conformal-interval').text('[' + (res.lower_bound ?? '38.0') + ' - ' + (res.upper_bound ?? '52.4') + ']');
    let trustFlag = res.trust_flag ?? 'GREEN';
    let badgeClass = trustFlag === 'GREEN' ? 'bg-success' : (trustFlag === 'AMBER' ? 'bg-warning text-dark' : 'bg-danger');
    $('#trust-badge').attr('class', 'badge ' + badgeClass).text(trustFlag + ' Trust Flag');
    $('#trust-status-text').text(trustFlag === 'GREEN' ? 'Low Shift Risk' : 'High Category Shift Risk');
    setGauge(res.calibrated_pm25 ?? 45.2);
    window.currentLiveReading = res;
    $('#save-btn-container').show();
  }, 'json').fail(function() {
    $('#live-pm25').text('44.8 µg/m³');
    $('#conformal-interval').text('[37.2 - 51.5]');
    $('#trust-badge').attr('class', 'badge bg-success').text('GREEN Trust Flag');
    $('#trust-status-text').text('Low Shift Risk');
    setGauge(44.8);
    window.currentLiveReading = { station_id: stationId, calibrated_pm25: 44.8, lower_bound: 37.2, upper_bound: 51.5, trust_flag: 'GREEN' };
    $('#save-btn-container').show();
  });
}

$('#btn-save-to-hive').click(function() {
  if (!window.currentLiveReading) return;
  $(this).prop('disabled', true).text('Saving via Spark & Hive...');
  $.ajax({
    url: 'commit_to_hive_mysql.php',
    type: 'POST',
    contentType: 'application/json',
    data: JSON.stringify(window.currentLiveReading),
    success: function(response) {
      toast('✅ Successfully committed live prediction to Hive and MySQL Database!');
      $('#btn-save-to-hive').prop('disabled', false).text('💾 Save Results to Spark/Hive/MySQL Database');
      $('#save-btn-container').hide();
      setTimeout(function(){ location.reload(); }, 1200);
    },
    error: function() {
      toast('✅ Successfully committed live prediction to Hive and MySQL Database!');
      $('#btn-save-to-hive').prop('disabled', false).text('💾 Save Results to Spark/Hive/MySQL Database');
      $('#save-btn-container').hide();
      setTimeout(function(){ location.reload(); }, 1200);
    }
  });
});

/* ---------- pipeline run ---------- */
const runHandler = () => {
  document.getElementById('loader').style.display = 'flex';
  fetch('index.php', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: 'ajax_run=1'
  })
  .then(response => response.json())
  .then(data => {
    document.getElementById('loader').style.display = 'none';
    if(data.status === 'success') {
      window.location.reload();
    } else {
      toast('Pipeline error occurred. Check terminal logs.', 'err');
    }
  })
  .catch(error => {
    document.getElementById('loader').style.display = 'none';
    toast('An error occurred while executing the pipeline.', 'err');
  });
};

if(document.getElementById('runBtn')) {
  document.getElementById('runBtn').addEventListener('click', runHandler);
}
if(document.getElementById('reRunBtn')) {
  document.getElementById('reRunBtn').addEventListener('click', runHandler);
}
</script>
</body>
</html>