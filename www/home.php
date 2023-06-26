<?php

$version = file_exists("pics/date.txt") ? file_get_contents("pics/date.txt") : "Empty";

echo($_SESSION['user_name']);

if($_SESSION['user_name']!="simon"){
//     header("Location: index.php");
//     exit();
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

     <form action="login.php" method="post">




     </form>

</body>

</html>