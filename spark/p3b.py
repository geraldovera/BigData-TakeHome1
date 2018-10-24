#Code here

>>> arecibo = escuelasdf.filter(escuelasdf.Region == 'Arecibo')
>>> resultb1 = arecibo.groupBy(arecibo.Distrito, arecibo.Ciudad).agg({"IdEscuela": "count"})
