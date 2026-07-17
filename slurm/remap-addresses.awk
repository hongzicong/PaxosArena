NR == FNR {
    address[$1] = $2
    next
}

{
    for (field = 1; field <= NF; field++) {
        if ($field in address) {
            $field = address[$field]
        }
    }
    print
}
