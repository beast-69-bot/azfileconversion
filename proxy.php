<?php
// PHP HLS Proxy for azhawasadda.in
$url = isset($_GET['url']) ? $_GET['url'] : '';
if (!$url) {
    header("HTTP/1.1 400 Bad Request");
    die("Error: No URL provided.");
}
if (!filter_var($url, FILTER_VALIDATE_URL)) {
    header("HTTP/1.1 400 Bad Request");
    die("Error: Invalid URL.");
}
// Fetch stream using cURL with FapHouse headers
$ch = curl_init();
curl_setopt($ch, CURLOPT_URL, $url);
curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
curl_setopt($ch, CURLOPT_TIMEOUT, 15);
curl_setopt($ch, CURLOPT_USERAGENT, 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36');
curl_setopt($ch, CURLOPT_REFERER, 'https://faphouse.com/');
$response = curl_exec($ch);
$content_type = curl_getinfo($ch, CURLINFO_CONTENT_TYPE);
$http_code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
curl_close($ch);
if ($http_code !== 200) {
    header("HTTP/1.1 " . $http_code);
    die("Error: Failed to fetch stream.");
}
// Enable CORS for our players
header("Access-Control-Allow-Origin: *");
header("Access-Control-Allow-Headers: *");
header("Content-Type: " . ($content_type ? $content_type : "application/vnd.apple.mpegurl"));
// If it's a playlist (.m3u8), rewrite segment links to route through this proxy
if (strpos($url, '.m3u8') !== false) {
    $base_url = substr($url, 0, strrpos($url, '/') + 1);
    $lines = explode("\n", $response);
    $new_lines = [];
    
    foreach ($lines as $line) {
        $line = trim($line);
        if (empty($line)) continue;
        
        if (strpos($line, '#') === 0) {
            $new_lines[] = $line;
        } else {
            // Rewrite URL to route through proxy
            if (strpos($line, 'http://') === 0 || strpos($line, 'https://') === 0) {
                $new_lines[] = "proxy.php?url=" . urlencode($line);
            } else {
                $absolute = $base_url . $line;
                $new_lines[] = "proxy.php?url=" . urlencode($absolute);
            }
        }
    }
    echo implode("\n", $new_lines);
} else {
    // If it's a video segment (.ts file), output directly
    echo $response;
}
?>
