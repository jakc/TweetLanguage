import arcpy
from arcpy import env
env.workspace = "C:\Users\sjackson\AppData\Roaming\ESRI\Desktop10.2\ArcCatalog\SQL 2012 lap-303629 SDE DC.sde"

#arcpy.CopyFeatures_management("majorrds.shp", "C:/output/output.gdb/majorrds2")
arcpy.DeleteFeatures_management("C:\Users\sjackson\AppData\Roaming\ESRI\Desktop10.2\ArcCatalog\SQL 2012 lap-303629 SDE DC.sde\sde_enabled.DBO.ArrowTrackingLayer_1")