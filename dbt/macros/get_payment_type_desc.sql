{#
    This macro will map the payment code to its description
#}

{% macro get_payment_type_desc(payment_type) -%}
    
    case {{ payment_type }}
        when 1 then 'CC'
        when 2 then 'Cash'
        when 3 then 'No Charge'
        when 4 then 'Dispute'
        when 5 then 'Unknowns'
        when 6 then 'Voided trip'
    end

{%- endmacro %}