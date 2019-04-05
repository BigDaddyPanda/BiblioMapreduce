/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bibliomapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author panda
 */
public class BiblioReducer
        extends Reducer<Text, Iterable<Text>, Text, Text> {
    

    public void reduce(Text key, Iterable<Text> values,
            Reducer.Context context
    ) throws IOException, InterruptedException {
        ArrayList<Text> result = new ArrayList<Text>();
        for (Text val : values) {
            result.add(val);
        } 
        context.write(key, result);
        /**
         * Traitement aprés le Reduce
         * on va stocker deux elements
         *  le nb d'empreints
         *  la liste d'empreinteurs
         */
        DataSeeder ds = new DataSeeder();
        ds.UpdateMyCollection("nombre_d_empreints", result.size(), "isbn", key.toString(), ds.Livres);
        ds.UpdateMyCollection("liste_d_empreinteurs", result, "isbn", key.toString(), ds.Livres);

    }
}
