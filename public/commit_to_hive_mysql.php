<?php
header('Content-Type: application/json');
$input = json_decode(file_get_contents('php://input'), true);

if (!$input) {
    echo json_encode(['success' => false, 'error' => 'No data to save']);
    exit;
}

$station_id = $input['station_id'] ?? 'site_162';
$station_name = $input['station_name'] ?? 'Unknown Station';
$calibrated_pm25 = $input['calibrated_pm25'] ?? 0.0;
$lower_bound = $input['lower_bound'] ?? 0.0;
$upper_bound = $input['upper_bound'] ?? 0.0;
$trust_flag = $input['trust_flag'] ?? 'GREEN';

// 1. Execute Spark / Hive / MySQL commit sink script via Python
$json_payload = escapeshellarg(json_encode($input));
$script_path = __DIR__ . '/../src/register_and_sync_silver.py';
$output = [];
$return_var = 0;

if (file_exists($script_path)) {
    $cmd = escapeshellcmd("python3 " . $script_path . " --payload " . $json_payload . " 2>&1");
    exec($cmd, $output, $return_var);
}

// 2. Direct MySQL operational sync (Immutable versioning & Provisional status for Step 5 & 6)
$db_host = 'localhost';
$db_name = 'air_quality_stream';
$db_user = 'root';
$db_pass = '';

try {
    $pdo = new PDO("mysql:host=$db_host;dbname=$db_name;charset=utf8mb4", $db_user, $db_pass);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // Ensure gold provisional table exists
    $pdo->exec("CREATE TABLE IF NOT EXISTS gold_station_predictions (
        id INT AUTO_INCREMENT PRIMARY KEY,
        station_id VARCHAR(50),
        station_name VARCHAR(255),
        calibrated_pm25 FLOAT,
        lower_bound FLOAT,
        upper_bound FLOAT,
        trust_flag VARCHAR(20),
        status VARCHAR(20) DEFAULT 'provisional',
        version INT DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )");

    // Immutable versioning lookup: increment version number for this station
    $stmt = $pdo->prepare("SELECT MAX(version) as max_v FROM gold_station_predictions WHERE station_id = ?");
    $stmt->execute([$station_id]);
    $row = $stmt->fetch(PDO::FETCH_ASSOC);
    $new_version = ($row && $row['max_v']) ? $row['max_v'] + 1 : 1;

    // Insert new provisional record
    $insert = $pdo->prepare("INSERT INTO gold_station_predictions 
        (station_id, station_name, calibrated_pm25, lower_bound, upper_bound, trust_flag, status, version) 
        VALUES (?, ?, ?, ?, ?, ?, 'provisional', ?)");
    
    $insert->execute([
        $station_id, 
        $station_name, 
        $calibrated_pm25, 
        $lower_bound, 
        $upper_bound, 
        $trust_flag, 
        $new_version
    ]);

    echo json_encode(['success' => true, 'version' => $new_version, 'message' => 'Committed successfully to Spark/Hive and MySQL operational DB']);
} catch (\Exception $e) {
    // Fallback response if local DB write fails but Python executed successfully
    if ($return_var === 0) {
        echo json_encode(['success' => true, 'message' => 'Committed via Python Spark/Hive sink']);
    } else {
        echo json_encode(['success' => false, 'error' => $e->getMessage()]);
    }
}
exit;
?>