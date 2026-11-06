<?php
error_reporting(0);
header('Content-Type: application/json');

$db_host = 'localhost';
$db_name = 'air_quality_stream';
$db_user = 'root';
$db_pass = '';

try {
    $pdo = new PDO("mysql:host=$db_host;dbname=$db_name;charset=utf8mb4", $db_user, $db_pass);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

    // Safely ensure columns exist upfront
    try {
        $pdo->exec("ALTER TABLE gold_station_predictions ADD COLUMN reference_pm25 FLOAT DEFAULT NULL");
    } catch (\Exception $e) {}
    try {
        $pdo->exec("ALTER TABLE gold_station_predictions ADD COLUMN residual_error FLOAT DEFAULT NULL");
    } catch (\Exception $e) {}

    // 1. Find provisional records that need ground truth matching
    $stmt = $pdo->query("SELECT * FROM gold_station_predictions WHERE status = 'provisional' LIMIT 5");
    $provisional_records = $stmt->fetchAll(PDO::FETCH_ASSOC);

    if (empty($provisional_records)) {
        echo json_encode([
            'success' => true, 
            'message' => 'No provisional records found to convert. Save some new predictions from the map first!'
        ]);
        exit;
    }

    $updated_count = 0;
    foreach ($provisional_records as $rec) {
        // Simulate official reference lab measurement (e.g., actual reading with minor variance)
        $simulated_reference_pm25 = round($rec['calibrated_pm25'] + (rand(-40, 40) / 10), 1);
        $residual = round($simulated_reference_pm25 - $rec['calibrated_pm25'], 2);

        // Update record: flip status to 'final', store reference value and residual error
        $update = $pdo->prepare("UPDATE gold_station_predictions 
            SET status = 'final', 
                reference_pm25 = ?, 
                residual_error = ? 
            WHERE id = ?");
        
        $update->execute([$simulated_reference_pm25, $residual, $rec['id']]);
        $updated_count++;
    }

    echo json_encode([
        'success' => true, 
        'message' => "Successfully processed reference feedback loop for {$updated_count} records. Status flipped to 'final' and residuals computed!"
    ]);

} catch (\Exception $e) {
    echo json_encode([
        'success' => false, 
        'message' => 'Error: ' . $e->getMessage()
    ]);
}
exit;
?>