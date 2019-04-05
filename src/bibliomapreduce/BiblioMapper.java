/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bibliomapreduce;

import com.mongodb.BasicDBObject;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author panda
 */
public class BiblioMapper extends Mapper<Object, BasicDBObject, Text, Text> {

    public void map(Object key, BasicDBObject value, Mapper.Context context
    ) throws IOException, InterruptedException {
        DataSeeder ds = new DataSeeder();
        
        String isbn = value.get("isbn").toString();
        String nom_emp = value.get("nom_emp").toString(); 
        context.write(new Text(isbn), new Text(nom_emp));
    }
}
