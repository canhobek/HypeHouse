SELECT *
FROM {{ source('public', 'comment_count_aws') }}