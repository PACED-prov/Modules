/*
 --------------------------------------------------------------------------------
 SPADE - Support for Provenance Auditing in Distributed Environments.
 Copyright (C) 2012 SRI International

 This program is free software: you can redistribute it and/or
 modify it under the terms of the GNU General Public License as
 published by the Free Software Foundation, either version 3 of the
 License, or (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program. If not, see <http://www.gnu.org/licenses/>.
 --------------------------------------------------------------------------------

 */
package spade.filter;

import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import spade.core.AbstractEdge;
import spade.core.Vertex;
import spade.core.Edge;
import spade.core.AbstractFilter;
import spade.core.AbstractVertex;
import spade.core.Settings;
import spade.utility.HelperFunctions;

/**
 * A filter to drop annotations passed in arguments.
 * 
 * Arguments format: keys=name,version,epoch
 * 
 * Note 1: Filter relies on two things for successful use of Java Reflection API:
 * 1) The vertex objects passed have an empty constructor
 * 2) The edge objects passed have a constructor with source vertex and a destination vertex (in that order)
 * 
 * Note 2: Creating copy of passed vertices and edges instead of just removing the annotations from the existing ones
 * because the passed vertices and edges might be in use by some other classes specifically the reporter that
 * generated them. In future, maybe shift the responsibility of creating a copy to Kernel before it sends out
 * the vertices and edges to filters.
 */


/*
 --------------------------------------------------------------------------------
 @What it does?
    The following filter serves the purpose of removing annotations from vetices
	and edges.
 
 @When should you use it?
    If you want to remove some annotations which you do not want to keep in the
	the vertices and edges.
	
	A more useful implementation comes out of the fact the SPADE collapse 
	vertices and edges with the same key-value pair of the annotations. 
	Therefore, if you have multiple graph objects that essentially (varies
	from contest to context) serve the same purpose but differ based on a few 
	dispensable annotations (for e.g. jiffies) then you can collapse these
	objects by making their annotation key-value pair the same. This can be done
	by dropping the differing annotations.
 
 @authors Shahpar Khan, Mashal Abbas, Abdul Monum
 --------------------------------------------------------------------------------
*/

public class DropKeys extends AbstractFilter{

	private Logger logger = Logger.getLogger(this.getClass().getName());

	private static final String
		keyEdgeDropKeys = "EdgeDropKeys",
		keyVertexDropKeys = "VertexDropKeys",
		keyKeepOriginalID = "KeepOriginalID";
	
	private Set<String> edgeDropKeys = new HashSet<String>();
	private Set<String> vertexDropKeys = new HashSet<String>();

	private Boolean keepOriginalID; 
	
	// private Set<String> keysToDrop = new HashSet<String>(); 
	
	public boolean initialize(String arguments){
		Map<String, String> configMap;
		final String configFilePath = Settings.getDefaultConfigFilePath(this.getClass());

		try{
			configMap = HelperFunctions.parseKeyValuePairsFrom(arguments, new String[]{configFilePath});
			
			if(configMap == null){
				throw new Exception("NULL config map read");
			}
		}
		catch (Exception e){
			logger.log(Level.SEVERE, "Failed to read config file", e);
			return false;
		}

		if(configMap.get(keyEdgeDropKeys) == null || configMap.get(keyEdgeDropKeys).trim().isEmpty() || configMap.get(keyVertexDropKeys) == null || configMap.get(keyVertexDropKeys).trim().isEmpty()){
			logger.log(Level.WARNING, "Must specify 'EdgeDropKeys' and 'VertexDropKeys' argument");
			return false;
		}else if(configMap.get(keyKeepOriginalID) == null || !(configMap.get(keyKeepOriginalID).equals("true") || configMap.get(keyKeepOriginalID).equals("false"))){
			logger.log(Level.WARNING, "Must specify 'KeepOriginalID' argument as either true or false");
			return false;
		}else{
			String [] keyTokens = configMap.get(keyEdgeDropKeys).split(",");
			for(String keyToken : keyTokens){
				if(keyToken.trim().isEmpty()){
					logger.log(Level.WARNING, "Empty key in 'EdgeDropKeys' argument. Invalid arguments");
					return false;
				}else{
					keyToken = keyToken.trim();
					//Must not be 'type'
					if(keyToken.equals("type")){
						logger.log(Level.WARNING, "Cannot remove 'type' key. Invalid arguments");
						return false;
					}else{
						edgeDropKeys.add(keyToken);
					}
				}
			}


			keyTokens = configMap.get(keyVertexDropKeys).split(",");
			for(String keyToken : keyTokens){
				if(keyToken.trim().isEmpty()){
					logger.log(Level.WARNING, "Empty key in 'EdgeDropKeys' argument. Invalid arguments");
					return false;
				}else{
					keyToken = keyToken.trim();
					//Must not be 'type'
					if(keyToken.equals("type")){
						logger.log(Level.WARNING, "Cannot remove 'type' key. Invalid arguments");
						return false;
					}else{
						vertexDropKeys.add(keyToken);
					}
				}
			}


			if(configMap.get(keyKeepOriginalID).equals("true")){
				keepOriginalID = true;
			}else if(configMap.get(keyKeepOriginalID).equals("false")){
				keepOriginalID = false;
			}else{
				logger.log(Level.WARNING, "Must specify 'KeepOriginalID' argument as either true or false");
				return false;
			}
		}
		
		return true;
		
	}

	@Override
	public void putVertex(AbstractVertex incomingVertex) {

		if(keepOriginalID){
			if(incomingVertex != null){
				AbstractVertex vertexCopy = createCopyWithoutKeys(incomingVertex, vertexDropKeys);
				if(vertexCopy != null){
					putInNextFilter(vertexCopy);
				}
			}else{
				logger.log(Level.WARNING, "Null vertex");
			}
		}else{
			AbstractVertex newVertex = createNewVertexWithoutRef(incomingVertex);

			putInNextFilter(newVertex);		
		}
		
	}

	@Override
	public void putEdge(AbstractEdge incomingEdge) {
		if(incomingEdge != null && incomingEdge.getChildVertex() != null && incomingEdge.getParentVertex() != null){
			if(keepOriginalID){
				AbstractEdge edgeCopy = createCopyWithoutKeys(incomingEdge, edgeDropKeys);
				if(edgeCopy != null){
					putInNextFilter(edgeCopy);
				}
			}else{
				AbstractEdge newEdge = new Edge(createNewVertexWithoutRef(incomingEdge.getChildVertex()), createNewVertexWithoutRef(incomingEdge.getParentVertex()));

				Map<String, String> newAnnotations = new HashMap<String, String>(); 

				Set<String> annotationKeys = incomingEdge.getAnnotationKeys();

				for (String key : annotationKeys) {
					if(!edgeDropKeys.contains(key) && !key.equals("id")){
						newAnnotations.put(key, incomingEdge.getAnnotation(key));                    
					}
				}    
				
				

				newEdge.addAnnotations(newAnnotations);

				newEdge.addAnnotation("id", newEdge.bigHashCode());

				putInNextFilter(newEdge);
			}


		}else{
			logger.log(Level.WARNING, "Invalid edge: {0}, source: {1}, destination: {2}", new Object[]{
					incomingEdge, 
					incomingEdge == null ? null : incomingEdge.getChildVertex(),
					incomingEdge == null ? null : incomingEdge.getParentVertex()
			});
		}
	}



	private AbstractVertex createNewVertexWithoutRef(AbstractVertex incomingVertex){
		AbstractVertex newVertex = new Vertex();
		
		Map<String, String> newAnnotations = new HashMap<String, String>(); 
			
		Set<String> annotationKeys = incomingVertex.getAnnotationKeys();

		for (String key : annotationKeys) {
			if(!vertexDropKeys.contains(key) && !key.equals("id")){
				newAnnotations.put(key, incomingVertex.getAnnotation(key));				
			}
		}
		
		newVertex.addAnnotations(newAnnotations);

		newVertex.addAnnotation("id", newVertex.bigHashCode());

		return newVertex;
	}
	
	/**
	 * Creates copy of vertex using Reflection API and removes the given keys afterwards
	 * 
	 * @param vertex vertex to create a copy of
	 * @param dropAnnotations annotations to remove
	 * @return copy of the vertex without the given annotations
	 */
	private AbstractVertex createCopyWithoutKeys(AbstractVertex vertex, Set<String> dropAnnotations){
		try{
			AbstractVertex vertexCopy = vertex.copyAsVertex();
			for(String dropAnnotation : dropAnnotations){
				vertexCopy.removeAnnotation(dropAnnotation);
			}
			return vertexCopy;
		}catch(Exception e){
			logger.log(Level.SEVERE, "Failed to initialize vertex: " + vertex, e);
			return null;
		}
	}
	
	/**
	 * Creates a deep copy of the edge (i.e. of source and destination vertices too) using Reflection API 
	 * and removes the given keys afterwards
	 * 
	 * @param edge edge to create a copy of
	 * @param dropAnnotations annotations to remove
	 * @return copy of the edge without the given annotations
	 */
	private AbstractEdge createCopyWithoutKeys(AbstractEdge edge, Set<String> dropAnnotations){
		try{
			AbstractVertex source = createCopyWithoutKeys(edge.getChildVertex(), dropAnnotations);
			AbstractVertex destination = createCopyWithoutKeys(edge.getParentVertex(), dropAnnotations);
			if(source == null){
				throw new Exception("Failed to create copy of source vertex");
			}
			if(destination == null){
				throw new Exception("Failed to create copy of destination vertex");
			}
			AbstractEdge edgeCopy = edge.getClass().getConstructor(edge.getChildVertex().getClass(),
					edge.getParentVertex().getClass()).newInstance(source, destination);
			edgeCopy.addAnnotations(edge.getCopyOfAnnotations());
			for(String dropAnnotation : dropAnnotations){
				edgeCopy.removeAnnotation(dropAnnotation);
			}
			return edgeCopy;
		}catch(Exception e){
			logger.log(Level.SEVERE, "Failed to initialize edge: " + edge, e);
			return null;
		}
	}
}