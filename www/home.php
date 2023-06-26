<?php
session_start();
$version = file_exists("pics/date.txt") ? file_get_contents("pics/date.txt") : "Empty";

if($_SESSION['user_name']!="simon"){
    header("Location: index.php");
    exit();
}

?>

<!DOCTYPE html>

<html>

<head>

    <title>Home</title>

    <link rel="stylesheet" type="text/css" href="style.css">

</head>

<body>
        <h2><?php echo($version); ?></h2>
<?php
        print_r($_GET);
        echo("pics/map/txt");
?>
</body>

</html>