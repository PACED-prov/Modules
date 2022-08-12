/*
 --------------------------------------------------------------------------------
 SPADE - Support for Provenance Auditing in Distributed Environments.
 Copyright (C) 2021 SRI International
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
package spade.transformer;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Collection;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import spade.core.AbstractEdge;
import spade.core.Edge;
import spade.core.AbstractTransformer;
import spade.core.AbstractVertex;
import spade.core.Graph;
import spade.core.Settings;
import spade.utility.HelperFunctions;
import spade.reporter.audit.OPMConstants;


/*
 --------------------------------------------------------------------------------
 @What it does?
    The following transformer serves the purpose of merging the similar vertices
    into a singular vertex and then adjusting the inbound and outbound edges. The
    transformer also deletes the annotations in the vertices which have differing
    annotations across similar vertices.
 
 @When should you use it?
    If the graph you are working on is condensed where there are multiple vertices
    that can be merged on a set of keys. Doing this increases readibility of the
    graph.
 
 @authors Shahpar Khan, Abdul Monum, Mashal Abbas, and Saadullah
 --------------------------------------------------------------------------------
*/

public class MergeVertex extends AbstractTransformer{

    private final static Logger logger = Logger.getLogger(MergeVertex.class.getName());
    private String[] mergeBaseKeys = null;

    //arguments will be the attribute based on which a vertex will be marked as equal to other, it can be a single key or comma separated keys
    public boolean initialize(String arguments){
        if(arguments == null || arguments.length() == 0){
            logger.log(Level.SEVERE, "No key is provided.");
            return false;
        }else{
            mergeBaseKeys = arguments.split(",");
        }

        return true;
    }

    @Override
    public LinkedHashSet<ArgumentName> getArgumentNames(){
        return new LinkedHashSet<ArgumentName>();
    }

    @Override
    public Graph transform(Graph graph, ExecutionContext context){
        
        
        // corresponding vertices map
        Map<String, AbstractVertex> mergedVertices = new HashMap<String, AbstractVertex>();

        // creating merge vertices
        for(AbstractVertex v : graph.vertexSet()){
            
            String hash = vertexHashForKeys(v);
	        if(hash == null || hash.length() == 0){continue;}

            if (mergedVertices.get(hash) == null){

                AbstractVertex vertex = v.copyAsVertex();
                mergedVertices.put(hash, vertex);
                
            } else {
                AbstractVertex mergedVertex = mergedVertices.get(hash);
                Map<String, String> mergedVertexAnnotations = mergedVertex.getCopyOfAnnotations();

                Map<String, String> newVertexAnnotations = v.getCopyOfAnnotations();
                
                // for-loop to remove annotations that have differing value across similar vertices 
                for(Map.Entry<String, String> set : mergedVertexAnnotations.entrySet()){
                    String mergedVertexKey = set.getKey();
                    String mergedVertexValue = set.getValue();

                    String newVertexValue = newVertexAnnotations.get(mergedVertexKey);

                    if((newVertexValue != null) && (mergedVertexValue != null) && !(newVertexValue.contains(mergedVertexValue))){
                        String removedKey = mergedVertex.removeAnnotation(mergedVertexKey);
                    }
                    
                }

                mergedVertices.put(hash, mergedVertex);

            }
		
        }

        Graph resultGraph = new Graph();

        // pushing all the merged vertices to the result graph
        for(Map.Entry<String, AbstractVertex> set : mergedVertices.entrySet()){

            AbstractVertex mergedVertex = set.getValue();
            resultGraph.putVertex(mergedVertex);            
        }


        for(AbstractEdge edge: graph.edgeSet()){
  
            AbstractEdge newEdge = createNewWithoutAnnotations(edge);
            boolean foundChild = false;
            boolean foundParent = false;


            // fixing the parent and child vertex of each edge
            // this is necessary due to merging of multiple vertices
            // finally adding the edge to result graph
            for(Map.Entry<String, AbstractVertex> set : mergedVertices.entrySet()){
        
                if((foundParent == true) && (foundChild == true)){
                    break;
                }

                AbstractVertex mergedVertex = set.getValue();
                String mergedVertexHash = set.getKey();

                if(newEdge.getChildVertex() != null){

                    String hash = vertexHashForKeys(newEdge.getChildVertex());
                    
                    if(hash != null && hash.equals(mergedVertexHash)) {
                        foundChild = true;
                        newEdge.setChildVertex(mergedVertex);
                    }
                    
                }

                if(newEdge.getParentVertex() != null){
                    String hash = vertexHashForKeys(newEdge.getParentVertex());

                    if(hash != null && hash.equals(mergedVertexHash)) {
                        foundParent = true;
                        newEdge.setParentVertex(mergedVertex);
                    }
                }
            }

            if(!(vertexHashForKeys(newEdge.getChildVertex()).equals(vertexHashForKeys(newEdge.getParentVertex()))) && (foundChild == true) && (foundParent == true)){
                resultGraph.putEdge(newEdge);
            }
	    }

        return resultGraph;
        
    }
    
    // function to create hash by concatenating the annotations
    private String vertexHashForKeys(AbstractVertex vertex){

	    String hash = new String();
        for (String key : mergeBaseKeys) {
	    String annotation = vertex.getAnnotation(key);
	    if(annotation != null && annotation.length() > 0) 
            hash += annotation + ",";
        }
        return hash;
    }
}
