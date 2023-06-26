<?php
ini_set('display_errors', '1');
ini_set('display_startup_errors', '1');
error_reporting(E_ALL);
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
    if(!isset($_GET['name'])){
        echo('<table>
        <tr><td><a href="?name=btc">BTC</a></td>
            <td><a href="?name=eth">ETH</a></td>
            <td><a href="?name=ada">ADA</a></td>
            <td><a href="?name=ape">APE</a></td>
        </tr>
        <tr><td><a href="?name=sand">SAND</a></td>
            <td><a href="?name=sol">SOL</a></td>
            <td><a href="?name=dot">DOT</a></td>
            <td><a href="?name=avax">AVAX</a></td>
        </tr>
        <tr><td><a href="?name=doge">DOGE</a></td>
            <td><a href="?name=link">LINK</a></td>
            <td><a href="?name=shiba">SHIBA</a></td>
            <td><a href="?name=mana">MANA</a></td>
        </tr>
        <tr><td><a href="?name=algo">ALGO</a></td>
            <td><a href="?name=atom">ATOM</a></td>
            <td><a href="?name=matic">MATIC</a></td>
            <td><a href="?name=LTC">ltc</a></td>
        </tr>
        <tr><td><a href="?name=eos">EOS</a></td>
            <td><a href="?name=uni">UNI</a></td>
            <td><a href="?name=vet">VET</a></td>
            <td><a href="?name=xrp">XRP</a></td>
        </tr>
        <tr><td colspan="4"><a href="?name=others">OTHERS</a></td>
        </tr>
        </table>');
        }
        else{
            echo('<center><table>');
            foreach (new SplFileObject("pics/map/".$_GET['name'].".txt") as $fname) {
                    echo('<tr><td><img src="pics/small/'.$fname.'"/></td></tr>');
            }
            echo('</table></center>');
        }
?>
</body>

</html>