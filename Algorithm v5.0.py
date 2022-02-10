#Set SOP Equivalent to a method for use in the loop.
def overlapPolyCount(inputFeature):
    prevUnion = "Union/buffersUnion.shp"
    if arcpy.Exists(prevUnion):
        arcpy.Delete_management(prevUnion)
    prevMultiSingle = "MultiSingle/multiSingleOutput.shp"
    if arcpy.Exists(prevMultiSingle):
        arcpy.Delete_management(prevMultiSingle)
    prevSpatJoin = "SpatJoin/spatialJoinOutput.shp"
    if arcpy.Exists(prevSpatJoin):
        arcpy.Delete_management(prevSpatJoin)
    prevOPC = "OPC/OPCDone.shp"
    if arcpy.Exists(prevOPC):
        arcpy.Delete_management(prevOPC)    
    unionOutput = "Union/buffersUnion.shp"
    arcpy.analysis.Union([inputFeature], unionOutput)
    multiSingleOutput = "MultiSingle/multiSingleOutput.shp"
    arcpy.management.MultipartToSinglepart(unionOutput, multiSingleOutput)
    spatialJoinOutput = "SpatJoin/spatialJoinOutput.shp"
    arcpy.analysis.SpatialJoin(multiSingleOutput, multiSingleOutput, spatialJoinOutput, "JOIN_ONE_TO_ONE", "KEEP_ALL", "", "ARE_IDENTICAL_TO")
    OPCOutput = "OPCDone.shp"
    arcpy.conversion.FeatureClassToFeatureClass(spatialJoinOutput, "OPC/", OPCOutput)
    delIdent = "OPC/OPCDone.shp"
    arcpy.DeleteIdentical_management(delIdent, ["ORIG_FID_1", "TARGET_FID"])
    #return spatialJoinOutput

#import relevant information
import time
myTime = time.localtime()
startTime = time.strftime("%H:%M:%S", myTime)
print "Start time: " + startTime
import arcpy
import csv
import os
from arcpy import env
print "Imports complete"

#set workspace, source, and destination folders
arcpy.env.overwriteOutput = True
env.scratchWorkspace = "F:\CIS\Programming Practicum\Workspace"
env.workspace = "F:\CIS\Programming Practicum\Workspace"
print "Work environment set"

#specify .csv column names for lat and long, then convert .csv to .shp
prevBuffer = "Points/inputPoints.shp"
if arcpy.Exists(prevBuffer):
    arcpy.Delete_management(prevBuffer)
inputFile = "400 by 7500 3.csv"
outputFolder = "Points/"
xValue = 'long'
yValue = 'lat'
layerName = "inputPoints.shp"
arcpy.MakeXYEventLayer_management(inputFile, xValue, yValue, layerName)
arcpy.FeatureClassToShapefile_conversion(layerName, outputFolder)
print "Converted data from .csv to shapefile"

#create a buffer of the points
prevBuffer = "Buffer/inputBuffer.shp"
if arcpy.Exists(prevBuffer):
    arcpy.Delete_management(prevBuffer)
buffInput = "Points/inputPoints.shp"
buffOutput = "Buffer/inputBuffer.shp"
buffDist = str(arcpy.da.SearchCursor(buffInput, ("t_range",)).next()[0])
arcpy.Buffer_analysis(buffInput, buffOutput, buffDist + " Meters")
print "Buffered the points to the range value set in t_range"

#add an short field in the buffers shapefile, called 'added'
bufferShp = "Buffer/inputBuffer.shp"
fieldName = arcpy.ValidateFieldName("added")
fieldList = arcpy.ListFields(bufferShp)
if fieldName not in fieldList:
    arcpy.AddField_management(bufferShp, fieldName, "SHORT")
print "Added field 'added' to buffer, set all values to 0"

#copy the original buffers field to the BaseData folder for comparison
origBuffer = "BaseData/baseBuffer.shp"
if arcpy.Exists(origBuffer):
    arcpy.Delete_management(origBuffer)
arcpy.management.CopyFeatures(bufferShp, "BaseData/baseBuffer.shp")
print "Starting buffer layer copied to BaseData folder"

i = 0
stopFlag = 0
print "Starting loop"
with arcpy.da.SearchCursor("Buffer/inputBuffer.shp", "added") as cursor:
    for row in cursor:
            
        #call overlapping polygon count method
        print "Starting OverlapPolyCount"
        overlapPolyCount(bufferShp)
        print "OverlapPolyCount done"
            
        #sort the table by Join_Count
        prevSort = "Sort/sortedTable.shp"
        if arcpy.Exists(prevSort):
            arcpy.Delete_management(prevSort)
        inputTable = "OPC/OPCDone.shp"
        outputTable = "Sort/sortedTable.shp"
        sortedTable = arcpy.management.Sort(inputTable, outputTable, [["Join_Count", "DESCENDING"]])
        print "Records sorted high to low based on Join_Count"

        #add 'Select' field to data, give each row an increasing value
        fieldName = arcpy.ValidateFieldName("Select")
        fieldList = arcpy.ListFields(sortedTable)
        if fieldName not in fieldList:
            arcpy.AddField_management(sortedTable, fieldName, "LONG")
        inc = 0
        with arcpy.da.UpdateCursor(sortedTable, ["Select"]) as cursor:
            for row in cursor:
                row[0] = inc
                inc += 1
                cursor.updateRow(row)
        del cursor

        #select the topmost row and make it its own file (topVal)
        prevTop = "RemovedTop/topVal.shp"
        if arcpy.Exists(prevTop):
            arcpy.Delete_management(prevTop)
        inputData = sortedTable
        outputMost = "topVal/"
        with arcpy.da.SearchCursor(inputData, 'Join_Count') as cursor:
            for row in cursor:
                if row[0] == 1:
                    arcpy.Select_analysis(inputData, outputMost + "topVal.shp", "Join_Count = 1")
                    print "     Last iteration, all remaining 'Join_Count' values are 1"
                    print "     all remaining polygons exported"
                    stopFlag = 1
                    break
                else:
                    #sql = '["Select"] = \'0\''
                    arcpy.Select_analysis(inputData, outputMost + "topVal.shp", '"Select" = 0')
                    break
        del cursor
        print "Topmost row exported to its own file"

        #select by location buffers (buffOutput) with topVal
        inLayer = buffOutput
        selectFeature = "TopVal/TopVal.shp"
        outputFeature = "Added/added.shp"
        arcpy.MakeFeatureLayer_management(inLayer, "inLayer")
        locSelect = arcpy.management.SelectLayerByLocation("inLayer", "INTERSECT", selectFeature)
        print "Selected buffers by location based on location of top value by 'Join_Count'"

        #update value of 'added' in selected rows to 1
        with arcpy.da.UpdateCursor(locSelect, ["added"]) as cursor:
            for row in cursor:
                row[0] = 1
                cursor.updateRow(row)
        del cursor
        print "Updated values of 'added' to 1 in selected buffers"

        #copy rows with 'added' value set to 1 to new file, delete from old
        arcpy.management.CopyFeatures(locSelect, outputFeature)
        arcpy.management.DeleteRows(locSelect)
        print "Copied buffers with 'added' value of 1 to new file, deleted from original buffers layer"

        #create a base shapefile to append information to on iteration 1
        if(i == 0):
            origBuff = "Append/appBuff.shp"
            if arcpy.Exists(origBuff):
                arcpy.Delete_management(origBuff)
            origTop = "Append/baseTop.shp"
            if arcpy.Exists(origTop):
                arcpy.Delete_management(origTop)
            arcpy.CreateFeatureclass_management("Append/", "appBuff.shp", "", "BaseData/baseBuffer.shp")
            arcpy.CreateFeatureclass_management("Append/", "baseTop.shp", "", "TopVal/topVal.shp")
            print "Created base shapefiles to append data to"


        #append new data to old data
        arcpy.management.Append(outputFeature, origBuff)
        arcpy.management.Append(selectFeature, origTop)
        print "Data appended to base files"

        #keep track of iterations and exit the loop if stopFlag is set to 1
        i += 1
        print "     Iteration " + str(i) + " completed"
        if stopFlag == 1:
            break

#delete extra fields created during processing
deleteFields = ["added", "FID_inpu_1", "lat_1", "long_1", "t_range_1", "BUFF_DIS_1", "ORIG_FID_1", "added_1", "Select"]
arcpy.management.DeleteField("Append/baseTop.shp", deleteFields)
print "Extra and duplicate table fields deleted"

#print ending time
myTime = time.localtime()
endTime = time.strftime("%H:%M:%S", myTime)
print "Start time: " + startTime
print "End time: " + endTime

#import .shp to arcmap
"""
fc = "H:/CIS/Programming Practicum/Workspace/input points.shp"
mxd = arcpy.mapping.MapDocument("Workspace")
df = arcpy.mapping.ListDataFrames(mxd)[0]
arcpy.MakeFeatureLayer_management(fc, "poly")
addLayer = arcpy.mapping.Layer("poly")
arcpy.mapping.AddLayer(df,addLayer)
print "Added shapefile to ArcMap"
"""
