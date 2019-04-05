/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bibliomapreduce;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author panda
 */
public class DataSeeder {

    public static MongoClient mongoClient;
    public static DB database;
    public static DBCollection Livres;
    public static DBCollection Empreints;

    public String databse_name = "biblio";

    private void DataSeederSingleton() {
        try {
            mongoClient = new MongoClient();
            database = mongoClient.getDB(databse_name);
            Livres = database.getCollection("livres");
            Empreints = database.getCollection("empreints");
        } catch (Exception ex) {
            Logger.getLogger(DataSeeder.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Missing Driver");
        }
    }

    public DataSeeder() {
        if (mongoClient == null
                || database == null
                || Livres == null
                || Empreints == null) {
            DataSeederSingleton();
        }
    }

    public BasicDBObject generate_bdbo_livre(String isbn, String auteur, String titre) {
        return new BasicDBObject("isbn", isbn)
                .append("auteur", auteur)
                .append("titre", titre);
    }

    public BasicDBObject generate_bdbo_empreint(String isbn, String nom_emp, String date_livraision) {
        return new BasicDBObject("isbn", isbn)
                .append("nom_emp", nom_emp)
                .append("date_livraision", date_livraision);
    }

    public void SeedMyData() {
        String[][] init_livres = {{"123-r", "Samir", "Les Nuits"}, {"321-a", "Lahnin", "Les Jours"}};
        String[][] init_empreints = {{"123-r", "Samir", new Date().toString()}, {"321-a", "Lahnin", new Date().toString()}};

        for (String[] item : init_livres) {
            Livres.insert(generate_bdbo_livre(item[0], item[1], item[2]));
        }
        for (String[] item : init_empreints) {
            Empreints.insert(generate_bdbo_empreint(item[0], item[1], item[2]));
        }

    }

    public void UpdateMyCollection(String updates_identifier, ArrayList<String> updates, String target_identifier, String target_id, DBCollection target_collection) {
        target_collection.update(new BasicDBObject(target_identifier, target_id),
                new BasicDBObject(updates_identifier, updates)
        );
    }

    public void UpdateMyCollection(String updates_identifier, Object updates, String target_identifier, String target_id, DBCollection target_collection) {
        target_collection.update(new BasicDBObject(target_identifier, target_id),
                new BasicDBObject(updates_identifier, updates)
        );
    }

}
