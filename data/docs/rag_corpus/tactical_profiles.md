# Perfiles tacticos e interpretacion contextual

Este documento ofrece criterios tacticos para interpretar los eventos del partido. El objetivo es que los textos automaticos conecten metricas con comportamientos de juego.

## Equipo dominante con balon

Un equipo puede considerarse dominante con balon cuando acumula pases, mantiene alto porcentaje de acierto y genera continuidad ofensiva. Si ademas aparecen conducciones, regates o tiros, el dominio no es solo circulacion: tambien hay progresion y amenaza.

Indicadores asociados:

- muchos pases;
- alto `pass_success_pct`;
- alto `offensive_index`;
- tiros o goles;
- eventos ofensivos distribuidos en varias ventanas;
- pocos despejes propios.

Lectura recomendada: el equipo controla el ritmo, instala posesiones y consigue avanzar. Si no hay tiros, matizar que el dominio fue territorial o de posesion, pero no necesariamente profundo.

## Equipo directo o de transicion

Un equipo con menos pases pero buenos tiros, conducciones o regates puede estar atacando de forma directa. No necesita dominar la posesion para generar peligro.

Indicadores asociados:

- pocos pases comparado con el rival;
- tiros en ventanas concretas;
- conducciones y regates exitosos;
- alto valor medio del evento;
- goles con bajo volumen total.

Lectura recomendada: eficacia en ataques rapidos o transiciones. No confundir menor posesion con menor peligro si el equipo finaliza mejor.

## Equipo que defiende mucho

Un indice defensivo alto puede indicar que el equipo esta siendo exigido. Si se acumulan despejes, bloqueos y presiones, probablemente pasa mucho tiempo defendiendo. Esto puede ser positivo si limita tiros, pero preocupante si el rival finaliza con frecuencia.

Indicadores asociados:

- alto `defensive_index`;
- muchos bloqueos y despejes;
- muchas presiones;
- muchos duelos;
- bajo `offensive_index`;
- bajo volumen de pases.

Lectura recomendada: el equipo esta sosteniendose defensivamente. Si tambien gana duelos e intercepta, se puede hablar de buena respuesta defensiva. Si solo despeja y bloquea, conviene hablar de resistencia bajo dominio rival.

## Equipo equilibrado

Un equipo equilibrado combina indice ofensivo alto con buen rendimiento defensivo. Esto puede indicar que recupera, gana disputas y despues progresa.

Indicadores asociados:

- alto `offensive_index`;
- duelos ganados;
- intercepciones exitosas;
- presiones con continuidad ofensiva;
- pases exitosos tras recuperar;
- tiros o llegadas.

Lectura recomendada: el equipo no solo defiende bien, sino que transforma esas acciones defensivas en presencia ofensiva. Este caso es especialmente fuerte cuando las ventanas muestran continuidad.

## Duelos como termometro del partido

Los duelos reflejan el tono competitivo. Muchos duelos pueden indicar partido fisico, presion, segundas jugadas o falta de control limpio. Ganar duelos es importante, pero debe conectarse con lo que ocurre despues.

Escenarios:

- duelos ganados + indice ofensivo alto: superioridad en disputa y capacidad para atacar;
- duelos ganados + indice ofensivo bajo: resistencia o trabajo defensivo sin progresion;
- duelos perdidos + rival con pases exitosos: dificultad para recuperar y frenar ataques;
- muchos duelos para ambos equipos: tramo abierto, fisico o de ritmo alto.

## Presion y recuperacion

La presion es valiosa cuando incomoda al rival y produce recuperaciones, errores o ataques propios. Una acumulacion de presiones sin continuidad puede reflejar esfuerzo, pero no dominio.

Lecturas:

- presiones + intercepciones + ataques posteriores: presion eficaz;
- presiones + faltas: presion agresiva o mal temporizada;
- presiones + bajo acierto rival en pase: presion que reduce limpieza en salida;
- presiones sin recuperacion: desgaste defensivo sin premio.

## Ventanas de partido

El analisis incremental debe comparar cada ventana con el contexto anterior. Una ventana aislada puede ser enganosa. Si un equipo encadena varias ventanas con alto volumen de pases e indice ofensivo, hay dominio sostenido. Si solo aparece en una ventana, puede ser un pico puntual.

Tipos de ventana:

- ventana de dominio: mas pases, mas acierto, indice ofensivo alto;
- ventana de resistencia: indice defensivo alto, despejes y bloqueos;
- ventana de transicion: pocos eventos pero tiros o gol;
- ventana fisica: muchos duelos y faltas;
- ventana plana: pocos eventos y sin tiros.

## Contexto de equipo

Los datos estaticos del equipo pueden ayudar a matizar el informe. Una racha positiva puede explicar confianza, pero no debe sustituir los eventos del partido. Lesiones acumuladas pueden contextualizar menor ritmo o menor profundidad de banquillo, pero el informe debe hablar siempre desde lo observado.

Si el contexto indica que un equipo llega con varias lesiones y en el partido acumula alto indice defensivo con poco ataque, una lectura prudente seria: "el equipo sostuvo fases defensivas exigentes, posiblemente condicionado por menor disponibilidad, aunque la evidencia principal procede de los eventos del partido".

## Reglas de interpretacion final

Para decidir una lectura tactica, combinar siempre al menos dos familias de indicadores:

- posesion: pases y acierto;
- amenaza: tiros, goles y valor;
- progresion: conducciones y regates;
- defensa: duelos, intercepciones, bloqueos, despejes;
- intensidad: volumen de eventos por ventana;
- contexto: racha, lesiones, localia o estadio.

La mejor narracion es la que une metricas complementarias y reconoce matices.
