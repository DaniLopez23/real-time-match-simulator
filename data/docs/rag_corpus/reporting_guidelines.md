# Guia de redaccion del informe

El informe automatico debe convertir metricas de streaming en una explicacion clara del partido. Debe separar datos observados, interpretacion y contexto RAG. No debe inventar sucesos que no aparezcan en los eventos.

## Principios de redaccion

1. Priorizar hechos medibles: eventos, tiros, goles, pases, porcentajes, duelos, presiones e indices.
2. Interpretar con prudencia: una metrica alta puede tener varias lecturas segun el resto del contexto.
3. Evitar conclusiones absolutas si los datos no las sostienen.
4. Explicar el "por que" de cada lectura usando una o dos metricas.
5. Usar lenguaje comprensible para un stakeholder no tecnico.

## Como hablar de dominio

Se puede hablar de dominio cuando coinciden varias senales:

- alto volumen de pases;
- porcentaje de acierto elevado;
- muchos eventos ofensivos;
- indice ofensivo superior al rival;
- presencia de tiros o llegadas;
- continuidad entre ventanas.

Ejemplo de lectura correcta: "El equipo tuvo mas control en este tramo porque acumulo mas pases, mantuvo un buen porcentaje de acierto y transformo esa circulacion en acciones ofensivas".

Si solo hay muchos pases pero pocos tiros, conviene hablar de posesion o control de ritmo, no necesariamente de amenaza. Si hay tiros pero pocos pases, conviene hablar de ataques directos o eficacia en transiciones.

## Como hablar de defensa

Un indice defensivo alto no siempre significa comodidad defensiva. Puede significar que el equipo defendio mucho. El texto debe distinguir:

- defensa eficaz: duelos ganados, intercepciones y pocas amenazas rivales;
- defensa exigida: muchos despejes, bloqueos y tiros rivales;
- presion alta: muchas presiones y recuperaciones posteriores;
- resistencia: alto trabajo defensivo sin control ofensivo posterior.

Si un equipo tiene alto indice defensivo y tambien alto indice ofensivo, la lectura puede ser que esta compitiendo bien en ambos lados: recupera, gana duelos y convierte esas acciones en ataques. Si el indice defensivo es alto pero el ofensivo bajo, la lectura debe apuntar a contencion o sufrimiento.

## Como hablar de duelos

Los duelos ganados ayudan a explicar intensidad y control de disputas. Un alto porcentaje de duelos ganados permite afirmar que el equipo esta imponiendose en acciones individuales. Si ademas sube el indice ofensivo, puede decirse que esas disputas estan alimentando la progresion o el ataque.

Si los duelos ganados son altos pero no hay tiros ni pases exitosos, el informe debe matizar: el equipo compite bien, pero no consigue transformar esa ventaja en dominio ofensivo.

## Como hablar de intensidad

Una ventana con muchos eventos debe describirse como tramo intenso. Para que esa intensidad tenga impacto competitivo, deben aparecer acciones como tiros, goles, presiones exitosas, duelos ganados, intercepciones o cambios claros en indices.

No todas las ventanas intensas son favorables para el mismo equipo. Si un equipo acumula acciones defensivas y el rival acumula acciones ofensivas, el texto debe explicar que hubo intensidad, pero con roles diferentes: uno ataco y otro resistio.

## Como usar el contexto RAG

El contexto RAG sirve para enriquecer la explicacion, no para crear hechos nuevos. Debe utilizarse para:

- explicar que significa una metrica;
- matizar interpretaciones;
- conectar indicadores entre si;
- sugerir una lectura tactica general.

No debe usarse para afirmar lesiones, rachas, goles, ocasiones o alineaciones si esos datos no estan en las fuentes estructuradas o en el contexto del equipo.

## Estructura recomendada del texto incremental

Cada bloque por ventana debe incluir:

- rango de minutos;
- equipo con mas actividad o dominio;
- una metrica ofensiva clave;
- una metrica defensiva clave si es relevante;
- lectura tactica breve;
- matiz si no hubo tiros o goles.

Ejemplo de estilo:

"Minutos 20-25: el tramo tuvo mayor peso para el equipo local, que sostuvo la posesion con mas pases y buen acierto. Aunque no genero una ventaja clara en goles, su indice ofensivo indica continuidad en campo rival. El rival, con mayor carga defensiva y varios duelos, resistio mas que domino."

## Red flags para el LLM

El informe no debe:

- decir que hubo dominio solo por un unico indicador;
- llamar "mejor defensa" a un equipo que simplemente despejo mucho bajo presion;
- inventar ocasiones claras si solo hay eventos sin tiro;
- copiar JSON, claves internas o listas de metricas;
- usar jerga excesiva;
- contradecir las metricas de Spark.

## Cierre para stakeholder

La conclusion debe responder tres preguntas:

- quien tuvo mas peso en el tramo;
- que indicador respalda esa lectura;
- si el dominio fue ofensivo, defensivo, de transicion o solo de resistencia.
