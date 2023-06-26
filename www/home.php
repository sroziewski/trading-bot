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

    <title>LOGIN</title>

    <link rel="stylesheet" type="text/css" href="style.css">

</head>

<body>
        <h2><?php echo($version); ?></h2>

        <table>
        <tr><td><a href="?name=btc">BTC</a></td>
            <td><a href="?name=eth">ETH</a></td>
            <td><a href="?name=ada">ADA</a></td>
            <td><a href="?name=ape">APE</a></td>
        </tr>
        </table>


</body>

</html>