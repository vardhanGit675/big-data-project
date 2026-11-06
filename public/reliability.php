<?php
// --- HIVE & MYSQL HYBRID DATA LOADER ---
// Architecture: Direct MySQL query for live saved records + Spark/Hive CSV fallback for trust evaluations

$trustRows = [];
$saved_records = [];

// 1. Load saved records directly from MySQL database so live saves appear instantly
$db_host = 'localhost';
$db_name = 'air_quality_stream';
$db_user = 'root';
$db_pass = '';

try {
    $pdo = new PDO("mysql:host=$db_host;dbname=$db_name;charset=utf8mb4", $db_user, $db_pass);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
    $saved_records = $pdo->query("SELECT * FROM gold_station_predictions ORDER BY created_at DESC LIMIT 50")->fetchAll(PDO::FETCH_ASSOC);
} catch (\Exception $e) {
    // Fallback to CSV export if MySQL is unavailable
    $goldExportFile = __DIR__ . '/../results/gold_predictions_export.csv';
    if (file_exists($goldExportFile) && ($handle = fopen($goldExportFile, "r")) !== FALSE) {
        $g_headers = fgetcsv($handle, 1000, ",", "\"", "\\");
        if ($g_headers) {
            $g_headers = array_map(function($h) { return trim(preg_replace('/^\xEF\xBB\xBF/', '', $h)); }, $g_headers);
        }
        while (($data = fgetcsv($handle, 1000, ",", "\"", "\\")) !== FALSE) {
            $data = array_map('trim', $data);
            if ($g_headers && count($data) === count($g_headers)) {
                $saved_records[] = array_combine($g_headers, $data);
            }
        }
        fclose($handle);
    }
}

// 2. Read trust layer evaluation results from Spark/Hive CSV export
$trustFile = __DIR__ . '/../results/trust_layer_eval.csv';
if (file_exists($trustFile) && ($handle = fopen($trustFile, "r")) !== FALSE) {
    $headers = fgetcsv($handle, 1000, ",", "\"", "\\");
    if ($headers) {
        $headers = array_map(function($h) { return trim(preg_replace('/^\xEF\xBB\xBF/', '', $h)); }, $headers);
    }

    $count = 0;
    while (($data = fgetcsv($handle, 1000, ",", "\"", "\\")) !== FALSE && $count < 50) {
        $data = array_map('trim', $data);
        if ($headers && count($data) === count($headers)) {
            $row = array_combine($headers, $data);
        } else {
            $row = [
                'predicted' => $data[3] ?? 0,
                'lower_bound' => $data[8] ?? 0,
                'upper_bound' => $data[9] ?? 0,
                'reliability_flag' => $data[10] ?? 'Green'
            ];
        }
        $trustRows[] = $row;
        $count++;
    }
    fclose($handle);
}

// Display-only summary counts
$cnt_final = 0; $cnt_prov = 0;
foreach ($saved_records as $r) { if (($r['status'] ?? 'provisional') === 'final') $cnt_final++; else $cnt_prov++; }
$t_green = $t_amber = $t_red = 0;
foreach ($trustRows as $r) {
    $f = $r['reliability_flag'] ?? 'Green';
    if (strcasecmp($f, 'Green') === 0) $t_green++; elseif (strcasecmp($f, 'Amber') === 0) $t_amber++; else $t_red++;
}
?>
<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Model Trust Layer - Reliability Badges & Feedback Loop</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&family=Space+Grotesk:wght@500;600;700&display=swap" rel="stylesheet">
<script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
<style>
:root{
  --bg:#070b17; --glass:rgba(255,255,255,.055); --glass-b:rgba(255,255,255,.11);
  --text:#e8ecf8; --muted:#8e9ab8; --accent:#6c8cff; --accent2:#22d3ee; --accent3:#a78bfa;
  --green:#22c55e; --amber:#f59e0b; --red:#ef4444;
}
*{box-sizing:border-box}
html{scroll-behavior:smooth}
body{font-family:'Inter',sans-serif;color:var(--text);background:var(--bg);min-height:100vh;padding:0 0 60px;overflow-x:hidden;position:relative}
body::before,body::after{content:"";position:fixed;z-index:-1;border-radius:50%;filter:blur(110px);opacity:.45;animation:float 18s ease-in-out infinite}
body::before{width:520px;height:520px;background:#3b5bff;top:-140px;left:-120px}
body::after{width:480px;height:480px;background:#0ea5b7;bottom:-140px;right:-100px;animation-delay:-9s}
.grid-bg{position:fixed;inset:0;z-index:-1;pointer-events:none;
  background-image:linear-gradient(rgba(255,255,255,.035) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.035) 1px,transparent 1px);
  background-size:44px 44px;mask-image:radial-gradient(ellipse at center,#000 30%,transparent 75%)}
@keyframes float{0%,100%{transform:translate(0,0) scale(1)}50%{transform:translate(60px,40px) scale(1.15)}}
h1,h2,h3,h4,h5,h6{font-family:'Space Grotesk',sans-serif}
.container{max-width:1240px}

.topbar{position:sticky;top:0;z-index:1000;backdrop-filter:blur(16px);background:rgba(7,11,23,.72);border-bottom:1px solid var(--glass-b)}
.brand{display:flex;align-items:center;gap:.7rem;font-family:'Space Grotesk';font-weight:700;font-size:1.1rem;color:#fff;text-decoration:none}
.brand-dot{width:34px;height:34px;border-radius:10px;display:grid;place-items:center;background:linear-gradient(135deg,var(--accent),var(--accent2));box-shadow:0 0 24px rgba(108,140,255,.55)}
.nav-pill{color:var(--text);text-decoration:none;font-size:.85rem;font-weight:500;padding:.45rem .95rem;border-radius:999px;border:1px solid var(--glass-b);background:var(--glass);transition:.25s}
.nav-pill:hover{background:rgba(108,140,255,.22);border-color:var(--accent);color:#fff;transform:translateY(-2px)}
.nav-pill.primary{background:linear-gradient(135deg,var(--accent),var(--accent3));border:none;color:#fff;box-shadow:0 6px 20px rgba(108,140,255,.4)}

.hero{padding:54px 0 26px}
.hero h1{font-size:clamp(1.9rem,4vw,3rem);font-weight:700;line-height:1.1;letter-spacing:-.02em}
.grad-text{background:linear-gradient(90deg,var(--accent2),var(--accent),var(--accent3));-webkit-background-clip:text;background-clip:text;color:transparent;background-size:200% auto;animation:shine 6s linear infinite}
@keyframes shine{to{background-position:200% center}}
.hero p{color:var(--muted);max-width:640px}
.live-chip{display:inline-flex;align-items:center;gap:.5rem;font-size:.78rem;font-weight:600;letter-spacing:.06em;text-transform:uppercase;padding:.35rem .85rem;border-radius:999px;background:rgba(34,197,94,.12);border:1px solid rgba(34,197,94,.35);color:#86efac}
.pulse{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 0 0 rgba(34,197,94,.7);animation:pulse 1.8s infinite}
@keyframes pulse{70%{box-shadow:0 0 0 10px rgba(34,197,94,0)}100%{box-shadow:0 0 0 0 rgba(34,197,94,0)}}

.glass{background:var(--glass);border:1px solid var(--glass-b);border-radius:20px;backdrop-filter:blur(14px);box-shadow:0 20px 50px rgba(0,0,0,.35);overflow:hidden;transition:transform .35s,border-color .35s,box-shadow .35s}
.glass:hover{border-color:rgba(108,140,255,.35)}
.glass-head{display:flex;align-items:center;justify-content:space-between;gap:1rem;flex-wrap:wrap;padding:1.05rem 1.4rem;border-bottom:1px solid var(--glass-b);background:rgba(255,255,255,.03)}
.glass-head h3{margin:0;font-size:1.05rem;font-weight:600;display:flex;align-items:center;gap:.6rem}
.glass-body{padding:1.4rem}
.ico{width:32px;height:32px;border-radius:9px;display:grid;place-items:center;font-size:1rem;background:linear-gradient(135deg,rgba(108,140,255,.3),rgba(34,211,238,.2))}

.stat{padding:1.3rem 1.4rem;position:relative;height:100%}
.stat:hover{transform:translateY(-6px);box-shadow:0 26px 60px rgba(108,140,255,.22)}
.stat .label{font-size:.76rem;text-transform:uppercase;letter-spacing:.09em;color:var(--muted);font-weight:600}
.stat .value{font-family:'Space Grotesk';font-size:2.3rem;font-weight:700;line-height:1.15;margin-top:.3rem}
.stat .sub{font-size:.8rem;color:var(--muted)}
.stat .glow{position:absolute;right:-24px;top:-24px;width:90px;height:90px;border-radius:50%;filter:blur(30px);opacity:.55}

.table{--bs-table-bg:transparent;--bs-table-color:var(--text);--bs-table-striped-bg:rgba(255,255,255,.035);--bs-table-striped-color:var(--text);--bs-table-hover-bg:rgba(108,140,255,.1);--bs-table-hover-color:#fff;border-color:var(--glass-b);margin:0;font-size:.9rem}
.table thead th{color:var(--muted);font-size:.72rem;text-transform:uppercase;letter-spacing:.09em;font-weight:600;border-bottom:1px solid var(--glass-b);padding:.9rem 1rem;background:rgba(255,255,255,.03)}
.table td{padding:.85rem 1rem;border-color:rgba(255,255,255,.06)}
.text-muted{color:var(--muted)!important}

.badge{font-weight:600;letter-spacing:.03em;padding:.5em .95em;border-radius:999px}
.badge.bg-success{background:rgba(34,197,94,.18)!important;color:#86efac;border:1px solid rgba(34,197,94,.45);box-shadow:0 0 14px rgba(34,197,94,.18)}
.badge.bg-warning{background:rgba(245,158,11,.18)!important;color:#fcd34d!important;border:1px solid rgba(245,158,11,.45);box-shadow:0 0 14px rgba(245,158,11,.18)}
.badge.bg-danger{background:rgba(239,68,68,.18)!important;color:#fca5a5;border:1px solid rgba(239,68,68,.45);box-shadow:0 0 14px rgba(239,68,68,.18)}
.badge.bg-light{background:rgba(255,255,255,.08)!important;color:var(--muted)!important;border:1px solid var(--glass-b)}
.badge.fs-6{font-size:.85rem!important}

.btn{border-radius:12px;font-weight:600;transition:.25s}
.btn-glow{background:linear-gradient(135deg,var(--accent),var(--accent3));color:#fff;border:0;box-shadow:0 10px 30px rgba(108,140,255,.45)}
.btn-glow:hover{transform:translateY(-3px);box-shadow:0 16px 40px rgba(108,140,255,.6);color:#fff}
.btn-glow:disabled{opacity:.75}

.alert-i,.alert-w{border-radius:14px;padding:.9rem 1.2rem}
.alert-i{background:rgba(34,211,238,.1);border:1px solid rgba(34,211,238,.35);color:#67e8f9}
.alert-w{background:rgba(245,158,11,.1);border:1px solid rgba(245,158,11,.4);color:#fcd34d}

#toast-wrap{position:fixed;right:20px;bottom:20px;z-index:10000;display:flex;flex-direction:column;gap:10px}
.toast-x{min-width:280px;max-width:380px;padding:.95rem 1.2rem;border-radius:14px;background:#10172e;border:1px solid var(--glass-b);box-shadow:0 18px 40px rgba(0,0,0,.5);animation:slideIn .45s cubic-bezier(.2,.8,.2,1);font-size:.9rem}
.toast-x.ok{border-left:4px solid var(--green)}.toast-x.err{border-left:4px solid var(--red)}
@keyframes slideIn{from{opacity:0;transform:translateX(60px)}to{opacity:1;transform:none}}

.reveal{opacity:0;transform:translateY(28px);transition:opacity .8s cubic-bezier(.2,.8,.2,1),transform .8s cubic-bezier(.2,.8,.2,1)}
.reveal.in{opacity:1;transform:none}
.d1{transition-delay:.08s}.d2{transition-delay:.16s}.d3{transition-delay:.24s}.d4{transition-delay:.32s}
</style>
</head>
<body>
<div class="grid-bg"></div>
<div id="toast-wrap"></div>

<nav class="topbar">
  <div class="container py-3 d-flex flex-wrap gap-3 justify-content-between align-items-center">
    <a href="index.php" class="brand"><span class="brand-dot">🌫️</span> AirTrust ML</a>
    <div class="d-flex flex-wrap gap-2 align-items-center">
      <a href="index.php" class="nav-pill">⬅ Map Dashboard</a>
      <a href="reliability.php" class="nav-pill primary">🛡️ Reliability Audit</a>
      <a href="health.php" class="nav-pill">💓 System Health</a>
      <a href="benchmarks.php" class="nav-pill">📊 Benchmarks</a>
      <a href="analytics.php" class="nav-pill">📈 Advanced Analytics</a>
    </div>
  </div>
</nav>

<div class="container">

  <section class="hero reveal">
    <span class="live-chip mb-3"><span class="pulse"></span> Trust Layer Active</span>
    <h1 class="mt-3">🛡️ Trust Layer: <span class="grad-text">Reference Feedback & Gold Audit</span></h1>
    <p class="mt-3 mb-4">Audit immutable gold records, provisional → final status and conformal reliability badges.</p>
    <button id="btn-trigger-feedback" class="btn btn-glow px-4 py-3">🔄 Simulate Reference Lab Data Arrival (Step 7)</button>
  </section>

  <!-- Summary cards -->
  <div class="row g-3 mb-4">
    <div class="col-6 col-lg-3 reveal d1"><div class="glass stat"><div class="glow" style="background:#6c8cff"></div>
      <div class="label">Gold Records</div><div class="value"><?= count($saved_records); ?></div><div class="sub">MySQL Live Store</div></div></div>
    <div class="col-6 col-lg-3 reveal d2"><div class="glass stat"><div class="glow" style="background:#22c55e"></div>
      <div class="label">Final</div><div class="value"><?= $cnt_final; ?></div><div class="sub"><?= $cnt_prov; ?> provisional</div></div></div>
    <div class="col-6 col-lg-3 reveal d3"><div class="glass stat"><div class="glow" style="background:#22d3ee"></div>
      <div class="label">Trust Evaluations</div><div class="value"><?= count($trustRows); ?></div><div class="sub">Batch CSV preview</div></div></div>
    <div class="col-6 col-lg-3 reveal d4"><div class="glass stat"><div class="glow" style="background:#f59e0b"></div>
      <div class="label">Green Badges</div><div class="value"><?= $t_green; ?></div><div class="sub"><?= $t_amber; ?> amber · <?= $t_red; ?> red</div></div></div>
  </div>

  <!-- Section A: Saved Gold Records & Provisional/Final Status Tracker -->
  <div class="glass mb-4 reveal">
    <div class="glass-head">
      <h3><span class="ico">💾</span> Gold Layer Records & Feedback Status (Live DB View)</h3>
      <span class="badge bg-light"><?= count($saved_records); ?> Total Records</span>
    </div>
    <?php if (empty($saved_records)): ?>
      <div class="glass-body"><div class="alert-i">
        No saved gold records found in database. Click stations on your main map and hit save to log entries.
      </div></div>
    <?php else: ?>
      <div class="table-responsive">
        <table class="table table-striped table-hover align-middle">
          <thead>
            <tr>
              <th>ID / Version</th><th>Station Name</th><th>Calibrated PM2.5</th><th>Reference Lab PM2.5</th>
              <th>Residual Error</th><th>Trust Flag</th><th>Status</th><th>Timestamp</th>
            </tr>
          </thead>
          <tbody>
            <?php foreach ($saved_records as $rec):
                $flag = $rec['trust_flag'] ?? 'GREEN';
                $badgeColor = (strcasecmp($flag, 'GREEN') === 0) ? 'success' : ((strcasecmp($flag, 'AMBER') === 0) ? 'warning text-dark' : 'danger');
                $status = $rec['status'] ?? 'provisional';
                $statusBadge = ($status === 'final') ? 'bg-success' : 'bg-warning text-dark';
            ?>
            <tr>
              <td class="fw-bold">#<?= htmlspecialchars($rec['id'] ?? $rec['version'] ?? '1'); ?></td>
              <td><?= htmlspecialchars($rec['station_name'] ?? 'Unknown'); ?></td>
              <td><?= round((float)($rec['calibrated_pm25'] ?? 0), 1); ?> µg/m³</td>
              <td><?= isset($rec['reference_pm25']) && $rec['reference_pm25'] !== '' ? round((float)$rec['reference_pm25'], 1) . ' µg/m³' : '<span class="text-muted">Pending</span>'; ?></td>
              <td><?= isset($rec['residual_error']) && $rec['residual_error'] !== '' ? round((float)$rec['residual_error'], 2) : '<span class="text-muted">--</span>'; ?></td>
              <td><span class="badge bg-<?= $badgeColor; ?>"><?= htmlspecialchars($flag); ?></span></td>
              <td><span class="badge <?= $statusBadge; ?>"><?= ucfirst(htmlspecialchars($status)); ?></span></td>
              <td class="text-muted small"><?= htmlspecialchars($rec['created_at'] ?? ''); ?></td>
            </tr>
            <?php endforeach; ?>
          </tbody>
        </table>
      </div>
    <?php endif; ?>
  </div>

  <!-- Section B: Batch Evaluation Trust Layer CSV Preview -->
  <div class="glass reveal">
    <div class="glass-head">
      <h3><span class="ico">📊</span> Batch Trust Layer Evaluation Preview (CSV)</h3>
    </div>
    <?php if (empty($trustRows)): ?>
      <div class="glass-body"><div class="alert-w">
        No rows found in <code>../results/trust_layer_eval.csv</code>.
      </div></div>
    <?php else: ?>
      <div class="table-responsive">
        <table class="table table-hover align-middle">
          <thead>
            <tr><th>Predicted PM2.5</th><th>Interval Lower</th><th>Interval Upper</th><th>Reliability Badge</th></tr>
          </thead>
          <tbody>
            <?php foreach ($trustRows as $row):
                $flag = $row['reliability_flag'] ?? 'Green';
                $badgeColor = (strcasecmp($flag, 'Green') === 0) ? 'success' : ((strcasecmp($flag, 'Amber') === 0) ? 'warning text-dark' : 'danger');
            ?>
            <tr>
              <td class="fw-semibold"><?= round((float)($row['predicted'] ?? 0), 1); ?></td>
              <td><?= round((float)($row['lower_bound'] ?? 0), 1); ?></td>
              <td><?= round((float)($row['upper_bound'] ?? 0), 1); ?></td>
              <td><span class="badge bg-<?= $badgeColor; ?> fs-6 px-3 py-2"><?= htmlspecialchars($flag); ?></span></td>
            </tr>
            <?php endforeach; ?>
          </tbody>
        </table>
      </div>
    <?php endif; ?>
  </div>
</div>

<script>
function toast(msg, type){
  var el = document.createElement('div');
  el.className = 'toast-x ' + (type === 'err' ? 'err' : 'ok');
  el.textContent = msg;
  document.getElementById('toast-wrap').appendChild(el);
  setTimeout(function(){ el.style.transition='.4s'; el.style.opacity='0'; el.style.transform='translateX(60px)'; }, 3600);
  setTimeout(function(){ el.remove(); }, 4100);
}

var io = new IntersectionObserver(function(entries){
  entries.forEach(function(e){ if(e.isIntersecting){ e.target.classList.add('in'); io.unobserve(e.target); } });
}, {threshold:.06});
document.querySelectorAll('.reveal').forEach(function(el){ io.observe(el); });

$('#btn-trigger-feedback').click(function() {
    $(this).prop('disabled', true).text('Processing Reference Lab Feedback...');
    $.ajax({
        url: 'process_reference.php',
        type: 'GET',
        dataType: 'json',
        success: function(response) {
            toast(response.message || 'Feedback processed successfully.');
            setTimeout(function(){ location.reload(); }, 1400);
        },
        error: function() {
            toast('Error executing feedback loop.', 'err');
            $('#btn-trigger-feedback').prop('disabled', false).text('🔄 Simulate Reference Lab Data Arrival');
        }
    });
});
</script>
</body>
</html>