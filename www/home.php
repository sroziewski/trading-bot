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
        <tr><td><a href="?name=btc"></a></td>
            <td><a href="?name=eth"></a></td>
            <td><a href="?name=ada"></a></td>
            <td><a href="?name=ape"></a></td>
        </tr>
        </table>


</body>

</html>