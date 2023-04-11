select s3path, as2."size", file_name
from file_metadata.aws_scrape as2 
where as2.bucket in ('kf-study-us-east-1-prd-sd-0tyvy1tw', 'kf-study-us-east-1-prd-sd-1p41z782', 'kf-study-us-east-1-prd-sd-46rr9zr6',
'kf-study-us-east-1-prd-sd-46sk55a3', 'kf-study-us-east-1-prd-sd-6fpyjqbr', 'kf-study-us-east-1-prd-sd-7nq9151j',
'kf-study-us-east-1-prd-sd-8y99qzjj', 'kf-study-us-east-1-dev-sd-9pyzahhe', 'kf-study-us-east-1-prd-sd-9pyzahhe-etl',
'kf-study-us-east-1-prd-sd-9pyzahhe', 'kf-strides-study-us-east-1-prd-sd-aq9kvn5p', 'kf-study-us-east-1-prd-sd-aq9kvn5p',
'kf-study-us-east-1-prd-sd-b8x3c1mx', 'kf-study-us-east-1-prd-sd-bhjxbdqk', 'kf-strides-study-us-east-1-prd-sd-bhjxbdqk',
'kf-study-us-east-1-prd-sd-dk0krwk8', 'kf-study-us-east-1-prd-sd-dypmehhf', 'kf-study-us-east-1-prd-sd-dz4gpqx6',
'kf-study-us-east-1-prd-sd-dztb5hrr', 'kf-study-us-east-1-prd-sd-jws3v24d', 'kf-study-us-east-1-prd-sd-nmvv8a1y',
'kf-study-us-east-1-prd-sd-p445achv', 'kf-study-us-east-1-prd-sd-pet7q6f2', 'kf-study-us-east-1-qa-sd-preasa7s',
'kf-study-us-east-1-prd-sd-preasa7s', 'kf-study-us-east-1-prd-sd-r0eprsgs', 'kf-study-us-east-1-prd-sd-rm8afw0r',
'kf-study-us-east-1-prd-sd-vttshwv4', 'kf-study-us-east-1-prd-sd-w0v965xz', 'kf-study-us-east-1-prd-sd-ygva0e1c',
'kf-study-us-east-1-prd-sd-ynssaphe', 'kf-study-us-east-1-prd-sd-z6mwd3h0', 'kf-study-us-east-1-prd-sd-zfgdg5ys');