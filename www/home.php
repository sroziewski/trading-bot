<?php

$version = file_exists("pics/date.txt") ? file_get_contents("pics/date.txt") : "Empty";

echo "<h1>$version</h1>"

?>