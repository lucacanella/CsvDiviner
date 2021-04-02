INSERT INTO tabella (
	  longnum     
	, numerointero
	, testo       
	, nullabileint
	, unadataora  
	, convirgola  
  
)
(
	SELECT CAST(longnum AS BIGINT)
		 , CAST(nullif(nullabileint, '') AS INT)
		 , TO_TIMESTAMP(nullif(unadataora, ''), 'YYYY-MM-DD''T''HH24:MI:SS')
		 , CAST(numerointero AS INT)
		 , CAST(convirgola AS NUMERIC)
	  FROM tabella_testo
)	
			
CREATE TABLE nometabella (
	id                  SERIAL primary KEY,
	longnum     		BIGINT NOT NULL,
	numerointero        INT NOT NULL,
	testo               VARCHAR(15) NOT NULL,
	nullabileint        INT,
	unadataora          TIMESTAMP,
	convirgola          NUMERIC NOT NULL
);
