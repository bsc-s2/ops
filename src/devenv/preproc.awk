BEGIN { ln = 1 }

{
    if ($1 == "source" && system("test -f '"$2"'") == 0) {
        print "#include '" $2 "' begin"
        while ((getline line < $2) > 0) {
            print line
        }
        close($2)
        print "#include '" $2 "' end"
    }
    else {
        print
    }
}
