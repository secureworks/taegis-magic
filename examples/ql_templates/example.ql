FROM alert 
WHERE
    ( 
        {{ ips | in('@ip') }} OR
        {{ domains | regex('@domain') }} 
    ) AND
    severity >= {{ severity }}
EARLIEST={{ earliest }}