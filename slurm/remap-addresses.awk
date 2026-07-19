NR == FNR {
    address[$1] = $2
    next
}

{
    gsub(/\r/, "")
    for (field = 1; field <= NF; field++) {
        if ($field in address) {
            $field = address[$field]
        }
    }
    print
}
