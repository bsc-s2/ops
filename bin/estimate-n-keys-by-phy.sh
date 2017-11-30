# dump keys, count prefix

/usr/local/mysql-5.7.13/bin/mysql -S /tmp/mysql-4402.sock -e 'select `key` from `baishan-edge`.phy_25000000 limit 1000000;' -sN \
| awk '{
    k = $1;
    k = substr(k, 0, 5);
    d[k] += 1;
}

END {
for (k in d)  {
    print k " " d[k]
}
}
'

# prefix count to percentage

cat dist-1m.txt  | awk '{ print $1 " 1/1000: " $2*1000/1000000 }' | sort > perc-1m.txt

# percentage to shards

cat perc-1m.txt | sort | awk '
BEGIN {
   tit = "-----"
   n = 0
}

{
   n += $3
   if ( n > 20 ) {
      print tit " " n
      tit = $1
      n = 0
   }
}

END {
  print tit " " n
}

'
