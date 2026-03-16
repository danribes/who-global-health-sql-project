# Project Brief (SQL)

Este documento resume que se espera de tu proyecto.
Leelo antes de empezar a programar.

## Pregunta guia
Que historia de negocio puedes explicar con tus tablas y como la soportas con SQL reproducible?

## Requisitos tecnicos (obligatorios)
1. Modelo en capas: staging, core y semantic.
2. Al menos 3 tablas relacionadas en core.
3. Al menos 2 vistas de negocio (`vw_*`).
4. Al menos 1 transaccion explicita (`START TRANSACTION ... COMMIT/ROLLBACK`).
5. Uso de SQL avanzado compatible con el motor elegido.
6. Si el motor soporta `PROCEDURE`, `FUNCTION` o `TRIGGER`, se valora su uso.
7. Si el motor no soporta alguna de esas piezas, debe justificarse una alternativa equivalente.

## Requisitos analiticos (obligatorios)
1. 8-12 consultas analiticas bien documentadas.
2. Al menos:
   - 2 agregaciones temporales
   - 2 consultas con CTE
   - 1 ranking o top-N por grupo
   - 1 caso de calidad de datos detectado y corregido

## Entrega
- SQL ejecutable.
- Motor SQL indicado claramente.
- README final.
- 5 minutos de demo: modelo, pipeline, insights y limitaciones.

## Antes de empezar, deberias tener claro
1. Que dataset vas a usar.
2. Que tablas son importantes y cuales no.
3. Cual es tu tabla principal de analisis.
4. Que 3 preguntas de negocio quieres responder.
5. Que motor SQL vas a utilizar.
