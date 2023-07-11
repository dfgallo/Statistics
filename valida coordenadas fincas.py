colombia_bounding_box = {'lat':(-4.2316872, 16.0571269),	'lon':(-82.1243666, -66.8511907)}

def aux_validar(x, limits):
  if limits[0]<= x and x <= limits[1]:
    return True
  return False

def arreglar_coordenadas(coord, colombia_bounding_box):
  #coord: vector de coordenadas (latitud, longitud)

  
  if ( aux_validar(coord[0], colombia_bounding_box['lat']) and aux_validar(coord[1], colombia_bounding_box['lon']) ):
    #Valida si la latitud y la longitud están dentro de los límites de Colombia. Si es así devuelve los mismos datos
    return coord
  elif (aux_validar(coord[0], colombia_bounding_box['lon']) and aux_validar(coord[1], colombia_bounding_box['lat'])):
    #Valida si la latitud y longitud están intercambiadas. Devuelve los valores intercambiados
    return [coord[1], coord[0]]
  elif (aux_validar(coord[0], colombia_bounding_box['lat']) and aux_validar(-1*coord[1], colombia_bounding_box['lon'])):
    #Valida si la longitud tiene el signo incorrecto
    return [coord[0], -1*coord[1]]
  elif (aux_validar(-1*coord[0], colombia_bounding_box['lon']) and aux_validar(coord[1], colombia_bounding_box['lat'])):
    #Valida si la latitud  tiene el signo incorrecto
    return [coord[1], -1*coord[0]]
  else :
    return [None, None]

from numpy import result_type

#Ejemplo uso funciones Asumiento que los datos están en el dataframe df_datos
df_datos[['lat', 'lon']] = df_datos[['latitud', 'longitud']].apply(arreglar_coordenadas, 
                                                                                         axis = 1,
                                                                                         result_type = 'expand', 
                                                                                         colombia_bounding_box = colombia_bounding_box)