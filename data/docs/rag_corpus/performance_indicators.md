# Indicadores de rendimiento deportivo

Este documento define como interpretar las metricas generadas por el pipeline de streaming. El objetivo es que el informe automatico no se limite a enumerar eventos, sino que traduzca los datos en una lectura futbolistica coherente.

## Dominio ofensivo

Una acumulacion alta de eventos ofensivos suele indicar que un equipo esta teniendo peso con balon. En este proyecto se consideran especialmente ofensivos los pases, conducciones, regates y tiros. Si un equipo combina muchos pases con un porcentaje alto de acierto, la lectura razonable es que tiene control de la posesion, capacidad para circular la pelota y continuidad en sus ataques.

El volumen de pases por si solo no siempre significa dominio. Debe interpretarse junto a otros indicadores:

- muchos pases y alto acierto: dominio estable, circulacion limpia y menos perdidas;
- muchos pases pero bajo acierto: intento de dominar, pero con imprecision o presion rival;
- pocos pases y muchos tiros: ataques directos, transiciones rapidas o eficacia puntual;
- muchas conducciones y regates: progresion individual, ruptura de lineas o necesidad de avanzar sin apoyo colectivo;
- tiros frecuentes: presencia en zonas de finalizacion y amenaza ofensiva real.

Un indice ofensivo alto indica que el equipo esta acumulando acciones de progresion y finalizacion. Si ese indice se apoya en tiros y goles, la interpretacion es mas fuerte: no solo hay posesion, tambien hay impacto competitivo.

## Calidad de la posesion

El porcentaje de exito en el pase ayuda a distinguir posesion util de posesion fragil. Un equipo con alta precision en pase suele tener mejor estructura, mejores apoyos y menos perdidas. Si ademas aparece un alto volumen de eventos ofensivos, puede hablarse de dominio territorial o control del ritmo del partido.

Cuando el porcentaje de pase baja, el informe debe evitar afirmar dominio claro aunque el volumen de eventos sea alto. En ese caso conviene hablar de ritmo, insistencia o actividad, pero no necesariamente control.

## Actividad defensiva

Un indice defensivo alto puede tener dos lecturas. Por un lado, puede mostrar que un equipo defiende bien porque gana duelos, intercepta, bloquea y despeja. Por otro lado, tambien puede indicar que esta obligado a defender mucho porque el rival le somete o le instala cerca de su area.

La interpretacion depende del contexto:

- indice defensivo alto con pocas ocasiones recibidas: defensa eficaz y control del riesgo;
- indice defensivo alto con muchos tiros rivales: resistencia defensiva bajo presion;
- muchos despejes y bloqueos: el equipo esta defendiendo cerca de su porteria;
- muchas intercepciones: lectura anticipativa y capacidad para cortar ataques;
- muchas presiones: intento de incomodar la salida rival o recuperar rapido tras perdida.

Por tanto, el informe debe evitar decir automaticamente que "defiende mejor" solo porque el indice defensivo sea alto. Debe matizar si ese indice refleja control defensivo o necesidad de defender.

## Duelos ganados

Los duelos ganados son un indicador de competitividad individual y solidez en disputas. Si un equipo gana muchos duelos y ademas tiene un indice ofensivo alto, la lectura puede ser muy positiva: esta defendiendo bien las disputas y convirtiendo esas recuperaciones o ventajas en ataques.

Si hay muchos duelos ganados pero bajo indice ofensivo, el equipo compite bien sin transformar esa ventaja en dominio ofensivo. En ese caso el informe puede hablar de resistencia, intensidad o buen trabajo sin balon, pero no de superioridad completa.

Si el porcentaje de duelos ganados es bajo y el rival acumula pases o tiros, la lectura probable es que el equipo esta perdiendo segundas jugadas, llegando tarde o teniendo dificultades para frenar la progresion rival.

## Presion, recuperacion y transiciones

Las presiones indican intento de incomodar al rival. Una presion exitosa tiene mas valor si deriva en recuperacion, tiro, pase progresivo o continuidad ofensiva. Muchas presiones sin recuperacion pueden significar esfuerzo defensivo alto, pero no necesariamente eficacia.

Cuando un equipo acumula presiones, duelos ganados e intercepciones, el informe puede describirlo como un tramo de intensidad defensiva. Si despues aparecen conducciones, regates, tiros o goles, puede hablarse de transicion exitosa: defender bien y atacar rapido.

## Tiros y goles

Los tiros indican produccion ofensiva. Un equipo que tira mucho esta llegando a zonas de finalizacion, pero la calidad del resultado depende del outcome:

- gol: maximo impacto;
- tiro a puerta: amenaza real;
- tiro bloqueado: llegada ofensiva frenada por defensa rival;
- tiro fuera: finalizacion sin precision.

Un alto volumen de tiros con pocos goles puede interpretarse como dominio sin eficacia. Pocos tiros con gol pueden indicar eficacia puntual, pero no necesariamente superioridad sostenida.

## Faltas y agresividad

Las faltas contextualizan interrupciones y agresividad defensiva. Muchas faltas pueden senalar dificultades para defender limpiamente, presion mal coordinada o necesidad de cortar transiciones. Tambien pueden aparecer en fases de alta disputa.

El informe debe relacionar faltas con duelos, presiones e indice defensivo. Muchas faltas y muchos duelos perdidos suelen indicar sufrimiento. Muchas faltas con duelos ganados pueden indicar un partido fisico y de mucho contacto.

## Jugador destacado

La participacion de un jugador se aproxima contando intervenciones: pases, tiros, conducciones, regates, presiones, duelos o acciones defensivas. Un jugador destacado no siempre es quien marca. Puede ser quien acumula participaciones relevantes, genera continuidad, gana duelos o sostiene al equipo defensivamente.

Para stakeholders no tecnicos, conviene explicar el protagonismo con una frase simple: que hizo, en que fase del juego impacto y que metrica lo respalda.
