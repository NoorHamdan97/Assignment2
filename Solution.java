
package olympic;

import olympic.business.Athlete;
import olympic.business.ReturnValue;
import olympic.business.Sport;
import olympic.data.DBConnector;
import olympic.data.PostgreSQLErrorCodes;

import java.sql.*;
import java.util.ArrayList;

import static olympic.business.ReturnValue.*;

public class Solution {
    public static void createTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{
            pstmt = connection
                    .prepareStatement("CREATE TABLE Athlete\n" + "(\n" + "    id integer,\n" + "    name text NOT NULL,\n"
                            + "    country text NOT NULL, \n" + "    active BOOLEAN NOT NULL default (false), \n"
                            + "    PRIMARY KEY (id),\n" + "    CHECK (id > 0)\n" + ")");


            pstmt.execute();
            pstmt.close();
            pstmt = null;

            pstmt = connection.prepareStatement("CREATE TABLE Sport\n" + "(\n" + "    id integer,\n"
                    + "    name text NOT NULL,\n"
                    + "    city text NOT NULL, \n" + "    athletesCount integer default (0), \n"
                    + "    PRIMARY KEY (id),\n" +
                    " CHECK (athletesCount > -1),\n" +
                    " CHECK (id > 0) \n" + ")");
            pstmt.execute();

//            pstmt = connection.prepareStatement(
//                    "CREATE TABLE Observer\n" + "(\n" + "    athleteId integer ,\n" + "    sportId integer ,\n"
//                            + "   FOREIGN KEY (athleteId)  references Athlete(id) on DELETE CASCADE,\n"
//                            + "   FOREIGN KEY (sportId)  references Sport(id) on DELETE CASCADE,\n"
//                            + "payment integer,\n"
//                            + "PRIMARY KEY (athleteId,sportId)\n" + ")");
//            pstmt.execute();

            pstmt = connection.prepareStatement(
                    "CREATE TABLE participate\n" + "(\n" + "    athleteId integer ,\n" + "    sportId integer ,\n"
                            + "   FOREIGN KEY (athleteId)  references Athlete(id) on DELETE CASCADE,\n"
                            + "   FOREIGN KEY (sportId)  references Sport(id) on DELETE CASCADE,\n"
                            + "active BOOLEAN NOT NULL default (false),\n"
                            + "payment integer CHECK  (payment > 0),\n"
                            + "place integer CHECK (place BETWEEN 1 and 3),\n"
                            + "point integer default (0),\n"
                            + "PRIMARY KEY (athleteId,sportId)\n" +
                            ")");
            pstmt.execute();

            pstmt = connection.prepareStatement(
                    "CREATE TABLE FriendShip\n" + "(\n" + "    Id1 integer ,\n" + "    Id2 integer ,\n"
                            +"CHECK (Id1 <> Id2),\n"
                            + "   FOREIGN KEY (Id1)  references Athlete(id) on DELETE CASCADE,\n"
                            + "   FOREIGN KEY (Id2)  references Athlete(id) on DELETE CASCADE,\n"
                            + "PRIMARY KEY (Id1,Id2)\n" +
                            ")");
            pstmt.execute();


        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void clearTables() {

        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try{

            pstmt = connection.prepareStatement("Delete from FriendShip;");
            pstmt.execute();
//            pstmt = connection.prepareStatement("Delete from Self;");
//            pstmt.execute();
//            pstmt = connection.prepareStatement("Delete from Friends;");
//            pstmt.execute();
            pstmt = connection.prepareStatement("Delete from participate;");
            pstmt.execute();

            pstmt = connection.prepareStatement("Delete from Athlete;");
            pstmt.execute();

            pstmt = connection.prepareStatement("Delete from Sport;");
            pstmt.execute();



        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void dropTables() {

        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
//            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS JJJJ");
//            pstmt.execute();
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS Friends");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS Self");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS FriendShip");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS participate");
            pstmt.execute();
//            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Observer");
//            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Athlete");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Sport");
            pstmt.execute();


        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public static ReturnValue addAthlete(Athlete athlete) {

        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        try {
            pstmt = connection.prepareStatement("INSERT INTO Athlete" + " VALUES (?, ? , ? , ?)");
            pstmt.setInt(1, athlete.getId());
            pstmt.setString(2, athlete.getName());
            pstmt.setString(3, athlete.getCountry());
            pstmt.setBoolean(4, athlete.getIsActive());
            pstmt.execute();
        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()
                    || Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                value = BAD_PARAMS;
            } else if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                value = ALREADY_EXISTS;
            } else {
                value = ERROR;
            }
            return value;
        } catch (NullPointerException e) {
            value = BAD_PARAMS;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return value;

    }
    /*******************************************/
    private static Athlete resToAthelete(ResultSet res) {
        if (res == null ) {
            return null;
        }

        try {
            res.next();
            Athlete athlete = new Athlete();
            athlete.setId(res.getInt("id"));
            athlete.setName(res.getString("name"));
            athlete.setCountry(res.getString("country"));
            athlete.setIsActive(res.getBoolean("active"));
            return athlete;
        } catch (SQLException e) {
            return Athlete.badAthlete();
        }
    }

    /*******************************************/

    public static Athlete getAthleteProfile(Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        Athlete athlete = null;
        ResultSet res = null;
        try {
            pstmt = connection.prepareStatement("select * from  Athlete where id=" + athleteId + ";");
            res = pstmt.executeQuery();
            athlete = resToAthelete(res);
            res.close();
            if (athlete == null || athlete == Athlete.badAthlete()) {
                return Athlete.badAthlete();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return athlete;
    }

    public static ReturnValue deleteAthlete(Athlete athlete) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        try {

            /*** check the +*/
            pstmt = connection.prepareStatement("DELETE FROM Athlete WHERE id = +" + athlete.getId() + ";");
            if (pstmt.executeUpdate() == 0)
                value = NOT_EXISTS;

        } catch (SQLException e) {
            return ERROR;

        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return value;
    }

    public static ReturnValue addSport(Sport sport) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        if (sport.equals(sport.badSport())) {
            value = BAD_PARAMS;
            return value;
        }
        try {
            pstmt = connection.prepareStatement("SELECT * FROM  Sport");
            pstmt = connection.prepareStatement("INSERT INTO Sport" + " VALUES (?,?,?,?)");
            pstmt.setInt(1, sport.getId());
            pstmt.setString(2, sport.getName());
            pstmt.setString(3, sport.getCity());
            pstmt.setInt(4, sport.getAthletesCount());
            pstmt.execute();

        } catch (SQLException e) {
//			System.out.print(e);
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()
                    || Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()) {
                value = BAD_PARAMS;
            } else if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                value = ALREADY_EXISTS;
            } else {
                value = ERROR;
            }
            return value;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return value;
    }
    /*****************************************************/

    private static Sport resTosport(ResultSet res) {
        if (res == null ) {
            return null;
        }
        try {
            res.next();
            Sport sp = new Sport();
            sp.setId(res.getInt("id"));
            sp.setName(res.getString("name"));
            sp.setCity(res.getString("city"));
            sp.setAthletesCount(res.getInt("athletesCount"));
            return sp;
        } catch (SQLException e) {
            return Sport.badSport();
        }
    }


    /********************************************************/

    public static Sport getSport(Integer sportId) {


        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        Sport sp = null;
        ResultSet res = null;
        try {
            pstmt = connection.prepareStatement("select * from  Sport where id=" + sportId + ";");
            res = pstmt.executeQuery();
            sp = resTosport(res);
            if (sp == null || sp == sp.badSport()) {
                return sp.badSport();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return sp;
    }

    public static ReturnValue deleteSport(Sport sport) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        try {

            pstmt = connection.prepareStatement("DELETE FROM Sport WHERE id = +" + sport.getId() + ";");
            if (pstmt.executeUpdate() == 0)
                value = NOT_EXISTS;

        } catch (SQLException e) {
            return ERROR;

        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return value;
    }

    public static ReturnValue athleteJoinSport(Integer sportId, Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        try {
            pstmt = connection.prepareStatement("SELECT * from Sport" + " where id =" + sportId);
            ResultSet res = pstmt.executeQuery();
            pstmt = connection.prepareStatement("SELECT * from Athlete" + " where id =" + athleteId);
            ResultSet res2 = pstmt.executeQuery();
            if (res == null || res.next() == false || res2 == null || res2.next() == false) {
                value = NOT_EXISTS;
            }
            else {

                pstmt = connection.prepareStatement("select active from Athlete where id="+athleteId);
                res=pstmt.executeQuery();
                res.next();
                boolean val=res.getBoolean("active");
                pstmt = connection.prepareStatement("INSERT INTO participate(athleteId,sportId,active)" + " VALUES (?,?,?)");
                pstmt.setInt(2, sportId);
                pstmt.setInt(1, athleteId);
                pstmt.setBoolean(3, val);
                pstmt.executeUpdate();
//                pstmt = connection.prepareStatement("select count(athleteId) from participate where active=true");
//                res=pstmt.executeQuery();
//                res.next();
//                int count=res.getInt("count");
//                //	System.out.print(count);
//
//                pstmt = connection.prepareStatement("UPDATE  Sport SET  athletesCount=? where id ="+sportId);
//                pstmt.setInt(1, count);
//                pstmt.executeUpdate();
//                pstmt = connection.prepareStatement("select * from participate where (active=false and (athleteId="+athleteId+
//                        "and sportId="+sportId+"))");
//                res=pstmt.executeQuery();
//                if (res.next() != false){
//                    res.next();
//                    pstmt = connection.prepareStatement("UPDATE  participate SET  payment=? where athleteId ="+athleteId+
//                            "and sportId="+sportId);
//                    pstmt.setInt(1, 100);
//                    pstmt.executeUpdate();
//                }


                pstmt = connection.prepareStatement("select * from participate where (active=false and (athleteId="+athleteId+
                        "and sportId="+sportId+"))");
                res=pstmt.executeQuery();
                if (res.next() != false){
                    res.next();
                    pstmt = connection.prepareStatement("UPDATE  participate SET  payment=? where athleteId ="+athleteId+
                            "and sportId="+sportId);
                    pstmt.setInt(1, 100);
                    pstmt.executeUpdate();
                }else{
                    pstmt = connection.prepareStatement("select athletesCount from Sport where id="+sportId);
                    res=pstmt.executeQuery();
                    res.next();
                    int count=res.getInt("athletesCount");
                    count=count+1;
                    pstmt = connection.prepareStatement("UPDATE  Sport SET  athletesCount=? where id ="+sportId);
                    pstmt.setInt(1, count);
                    pstmt.executeUpdate();

                }



                res.close();
                res2.close();

                }

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                value = NOT_EXISTS;
            } else if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                value = ALREADY_EXISTS;
            } else {
                value = ERROR;
            }
        } finally {
            try {
                pstmt.close();

            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return value;
    }

    public static ReturnValue athleteLeftSport(Integer sportId, Integer athleteId) {


        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Athlete where id=" + athleteId);
            ResultSet resAth = pstmt.executeQuery();
            pstmt = connection.prepareStatement("SELECT * FROM Sport where id=" + sportId);
            ResultSet resSp = pstmt.executeQuery();
            if (resAth == null || resAth.next() == false || resSp == null || resSp.next() == false) {
                value = NOT_EXISTS;
            } else {

                pstmt = connection.prepareStatement("SELECT * FROM participate where athleteId=" + athleteId + "and sportId=" + sportId+"and active=false");
                ResultSet res = pstmt.executeQuery();
                if (res == null || res.next() == false) {

                    pstmt = connection.prepareStatement("SELECT * FROM participate where athleteId=" + athleteId + "and sportId=" + sportId+"and active=true");
                    res = pstmt.executeQuery();
                    if (res == null || res.next() == false) {
                        value = NOT_EXISTS;
                    }else{
                        pstmt = connection.prepareStatement("DELETE FROM participate where (athleteId=" + athleteId
                                + " and sportId=" + sportId + ");");
                        pstmt.executeUpdate();
                        pstmt = connection.prepareStatement("select athletesCount from Sport where id="+sportId);
                        res=pstmt.executeQuery();
                       res.next();
                        int count=res.getInt("athletesCount");
                        count=count-1;
                        pstmt = connection.prepareStatement("UPDATE  Sport SET  athletesCount=? where id ="+sportId);
                        pstmt.setInt(1, count);
                        pstmt.executeUpdate();
                    }



                }else{
                    pstmt = connection.prepareStatement("DELETE FROM participate where (athleteId=" + athleteId
                                + " and sportId=" + sportId + ");");
                        pstmt.executeUpdate();
                }

//                    pstmt = connection.prepareStatement("SELECT * FROM participate where athleteId=" + athleteId + "and sportId=" + sportId);
//                     ResultSet res = pstmt.executeQuery();
//                    if (res == null || res.next() == false) {
//                        value = NOT_EXISTS;
//                    } else {
//                        pstmt = connection.prepareStatement("DELETE FROM participate where (athleteId=" + athleteId
//                                + " and sportId=" + sportId + ");");
//                        pstmt.executeUpdate();
//                       // pstmt = connection.prepareStatement("select count(athleteId) from participate where active=true and sportId="+sportId);
//                        pstmt = connection.prepareStatement("select athletesCount from Sport where sportId="+sportId);
//
//                        res=pstmt.executeQuery();
//                        res.next();
//                        int count=res.getInt("count");
//                      //  System.out.print(count);
//                        pstmt = connection.prepareStatement("UPDATE  Sport SET  athletesCount=? where id ="+sportId);
//                        pstmt.setInt(1, count);
//                        pstmt.executeUpdate();

                    }




        }catch (SQLException e) {
            value = ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return value;

    }

    public static ReturnValue confirmStanding(Integer sportId, Integer athleteId, Integer place) {


        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        try {

            pstmt = connection.prepareStatement("SELECT * FROM Athlete where id=" + athleteId);
            ResultSet resAth = pstmt.executeQuery();
            pstmt = connection.prepareStatement("SELECT * FROM Sport where id=" + sportId);
            ResultSet resSp = pstmt.executeQuery();
            if (resAth == null || resAth.next() == false || resSp == null || resSp.next() == false) {
                value = NOT_EXISTS;
            }
            else{
                pstmt = connection.prepareStatement("SELECT * FROM participate where athleteId=" + athleteId + " and sportId="+sportId+"and active=true");
                ResultSet res = pstmt.executeQuery();
                if(res == null || res.next() == false){
                    value = NOT_EXISTS;
                }
                else{
                    pstmt = connection.prepareStatement("UPDATE  participate SET  place=? where athleteId ="+athleteId+" and sportId="+sportId);
                    pstmt.setInt(1,place);
                    pstmt.executeUpdate();
                    pstmt = connection.prepareStatement("UPDATE  participate SET  point=? where athleteId ="+athleteId+" and sportId="+sportId);
                    pstmt.setInt(1,4-place);
                    pstmt.executeUpdate();
                }

            }

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()){
                value=BAD_PARAMS;
            }else {
                value = ERROR;
            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return value;

    }

    public static ReturnValue athleteDisqualified(Integer sportId, Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Athlete where id=" + athleteId);
            ResultSet resAth = pstmt.executeQuery();
            pstmt = connection.prepareStatement("SELECT * FROM Sport where id=" + sportId);
            ResultSet resSp = pstmt.executeQuery();
            if (resAth == null || resAth.next() == false || resSp == null || resSp.next() == false) {
                value = NOT_EXISTS;
            }
            else{
                pstmt = connection.prepareStatement("SELECT * FROM participate where athleteId=" + athleteId + "and sportId="+sportId);
                ResultSet res = pstmt.executeQuery();
                if(res == null || res.next() == false){
                    value = NOT_EXISTS;
                }
                else{
                    pstmt = connection.prepareStatement("UPDATE  participate SET  place=?,point=? where athleteId ="+athleteId+"and sportId="+sportId);
                    pstmt.setNull(1, Types.INTEGER);
                    pstmt.setInt(2,0);
                    pstmt.executeUpdate();
                }
            }
        } catch (SQLException e) {
            value = ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return value;
    }

    public static ReturnValue makeFriends(Integer athleteId1, Integer athleteId2) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        try {
//            if(athleteId1 == athleteId2){
//                value=BAD_PARAMS;
//            }
          //  else{
//                pstmt = connection.prepareStatement("SELECT * FROM Athlete where id=" + athleteId1);
//                ResultSet resAth1 = pstmt.executeQuery();
//                pstmt = connection.prepareStatement("SELECT * FROM Athlete where id=" + athleteId2);
//                ResultSet resAth2 = pstmt.executeQuery();
//                if (resAth1 == null || resAth1.next() == false || resAth2 == null || resAth2.next() == false) {
//                    value = NOT_EXISTS;
//                }
//                else{
                    pstmt = connection.prepareStatement("SELECT * FROM FriendShip where Id1=" + athleteId1+"and Id2="+athleteId2);
                    ResultSet resAth = pstmt.executeQuery();
                    if (resAth == null || resAth.next() == false ) {
                        pstmt = connection.prepareStatement("INSERT INTO FriendShip" + " VALUES (?,?)");
                        pstmt.setInt(1,athleteId1);
                        pstmt.setInt(2,athleteId2);
                        pstmt.executeUpdate();
                        pstmt = connection.prepareStatement("INSERT INTO FriendShip" + " VALUES (?,?)");
                        pstmt.setInt(1,athleteId2);
                        pstmt.setInt(2,athleteId1);
                        pstmt.executeUpdate();

                    }
                    else{
                        value=ALREADY_EXISTS;
                    }

               // }
         //   }

        } catch (SQLException e) {
            if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()) {
                value = NOT_EXISTS;
            } else if (Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()) {
                value = ALREADY_EXISTS;
            } else if(Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                value = BAD_PARAMS;
            } else{
                value = ERROR;
            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return value;    }

    public static ReturnValue removeFriendship(Integer athleteId1, Integer athleteId2) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        try{
            pstmt = connection.prepareStatement("SELECT * FROM Athlete where id=" + athleteId1);
            ResultSet resAth1 = pstmt.executeQuery();
            pstmt = connection.prepareStatement("SELECT * FROM Athlete where id=" + athleteId2);
            ResultSet resAth2 = pstmt.executeQuery();
            if (resAth1 == null || resAth1.next() == false || resAth2 == null || resAth2.next() == false) {
                value = NOT_EXISTS;
            }
            else{
                pstmt = connection.prepareStatement("SELECT * FROM FriendShip where Id1=" + athleteId1+" and Id2="+athleteId2);
                ResultSet resAth = pstmt.executeQuery();
                if (resAth == null || resAth.next() == false ) {
                    value = NOT_EXISTS;
                }
                else{
                    pstmt = connection.prepareStatement("DELETE FROM FriendShip where (Id1=" + athleteId1
                            + " and Id2=" + athleteId2+ ");");
                    pstmt.executeUpdate();
                    pstmt = connection.prepareStatement("DELETE FROM FriendShip where (Id1=" + athleteId2
                            + " and Id2=" + athleteId1+ ");");
                    pstmt.executeUpdate();
                }

            }

        } catch (SQLException e) {
            value = ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return value;
    }

    public static ReturnValue changePayment(Integer athleteId, Integer sportId, Integer payment) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Athlete where id=" + athleteId);
            ResultSet resAth = pstmt.executeQuery();
            pstmt = connection.prepareStatement("SELECT * FROM Sport where id=" + sportId);
            ResultSet resSp = pstmt.executeQuery();
            if (resAth == null || resAth.next() == false || resSp == null || resSp.next() == false) {
                value = NOT_EXISTS;
            }
            else{
                pstmt = connection.prepareStatement("SELECT * FROM participate where athleteId=" + athleteId + "and sportId="+sportId+"and active=false");
                ResultSet res = pstmt.executeQuery();
                if(res == null || res.next() == false){
                    value = NOT_EXISTS;
                }
                else{
                    pstmt = connection.prepareStatement("UPDATE  participate SET  payment=? where athleteId ="+athleteId+"and sportId="+sportId);
                    pstmt.setInt(1,payment);
                    pstmt.executeUpdate();
                }

            }


        } catch (SQLException e) {
            if(Integer.valueOf(e.getSQLState()) == PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()) {
                value = BAD_PARAMS;
            }else {
                value = ERROR;
            }
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return value;

    }

    public static Boolean isAthletePopular(Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Athlete where id="+athleteId);
            ResultSet res = pstmt.executeQuery();
            if (res == null || res.next() == false) {
                return false;
            }
            pstmt = connection.prepareStatement("CREATE VIEW Friends AS SELECT sportId FROM participate WHERE athleteId IN("
                    + " SELECT Id2 FROM FriendShip WHERE Id1="+athleteId+")");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE VIEW Self AS SELECT sportId FROM participate WHERE athleteId="+athleteId);
            pstmt.execute();
            pstmt = connection.prepareStatement("SELECT * FROM Friends WHERE NOT EXISTS "
                    + "( SELECT * FROM Self WHERE (Self.sportId=Friends.sportId))");
             res = pstmt.executeQuery();
            if (res == null || res.next() == false) {
                pstmt = connection.prepareStatement("DROP VIEW IF EXISTS Friends");
                pstmt.execute();
                pstmt = connection.prepareStatement("DROP VIEW IF EXISTS Self");
                pstmt.execute();
                return true;
            }
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS Friends");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS Self");
            pstmt.execute();
            return false;
        } catch (SQLException e) {
            return false;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static Integer getTotalNumberOfMedalsFromCountry(String country) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        try{
        pstmt=connection.prepareStatement("select count(place) from (select * from participate where athleteId IN(select id from Athlete where country='"+country+"')) AS temp where  (active=true and place IS NOT NULL )");
        ResultSet res = pstmt.executeQuery();
        if(res == null || res.next()==false){
            return 0;
        }else {
           // res.next();
            return res.getInt("count");
        }
        } catch (SQLException e) {
           return 0;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static Integer getIncomeFromSport(Integer sportId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        try{
        pstmt=connection.prepareStatement("select SUM(payment) AS s from (select payment from participate where sportId="+sportId+" and active=false) AS temp");
        ResultSet res = pstmt.executeQuery();
            if(res == null || res.next()==false){
                return 0;
            }else {
                return res.getInt("s");
            }
        } catch (SQLException e) {
            return 0;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public static String getBestCountry() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ReturnValue value = OK;
        try{
            pstmt = connection.prepareStatement("select country from (select count(id) AS c,country from (select id,country from Athlete where id IN(" +
                    "(select athleteId from participate where (active =true and place IS NOT NULL)))) AS tmp GROUP BY country ORDER BY c DESC,country ASC LIMIT 1) AS tmp2 ");
            ResultSet res = pstmt.executeQuery();
            if(res == null || res.next() == false){
                return "";
            }else{
                String s= res.getString("country");
                return s;
            }
        } catch (SQLException e) {
            return null;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static String getMostPopularCity() {

        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Sport");
            ResultSet res = pstmt.executeQuery();
            if (res == null || res.next() == false) {
                return "";
            }
            pstmt = connection.prepareStatement("CREATE VIEW A AS SELECT city,SUM(athletesCount) as athNum FROM Sport GROUP BY city");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE VIEW B AS SELECT city,count(id) as spoNum FROM Sport GROUP BY city");
            pstmt.execute();
            pstmt = connection.prepareStatement("CREATE VIEW CTable AS SELECT A.city as city, A.athNum AS num1,B.spoNum As num2 FROM A , B WHERE A.city=B.city");
            pstmt.execute();

            pstmt = connection.prepareStatement("SELECT city, num1 ,num2,((num1*1.0)/num2) AS average  FROM CTable ORDER BY average DESC,city DESC LIMIT 1");
            res = pstmt.executeQuery();
            res.next();
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS CTable");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS A");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS B");
            pstmt.execute();

            String s=res.getString("city");
           // System.out.print(s);
            return s;
        } catch (SQLException e) {
            return null;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public static ArrayList<Integer> getAthleteMedals(Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> listId = new ArrayList<Integer>();
        try {
            pstmt = connection
                    .prepareStatement("CREATE VIEW tmp AS SELECT place,count(athleteId) AS c FROM participate WHERE (athleteId="+athleteId+" and place IS NOT NULL) GROUP BY place");
            pstmt.execute();
                for(int i=1;i<4;i++) {
                    pstmt = connection
                            .prepareStatement("SELECT c  FROM tmp WHERE place=" + i);
                  ResultSet  results = pstmt.executeQuery();
                    if (results == null || results.next() == false) {
                        listId.add(0);
                    } else {
                        listId.add(results.getInt("c"));
                    }
                }
                pstmt = connection.prepareStatement("DROP VIEW IF EXISTS tmp");
                pstmt.execute();

        } catch (SQLException e) {
            listId.add(0);
            listId.add(0);
            listId.add(0);
            return listId;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return listId;

    }

    public static ArrayList<Integer> getMostRatedAthletes() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> listId = new ArrayList<Integer>();
        try {
            pstmt = connection
                    .prepareStatement("SELECT athleteId FROM "
                            + " (SELECT athleteId, SUM(point) as Athrate FROM participate "
                            + " GROUP BY athleteId ) as ff "
                            + " ORDER BY Athrate DESC, athleteId ASC LIMIT 10");

            ResultSet results = pstmt.executeQuery();
            if (results == null || results.next() == false) {
                ;
            } else {
                Integer ids = results.getInt("athleteId");
                while (ids != null) {
                    listId.add(ids);
                    if (results.next() == false)
                        break;
                    ids = results.getInt("athleteId");
                }
                results.close();
            }
        } catch (SQLException e) {
            return listId;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return listId;
    }

    public static ArrayList<Integer> getCloseAthletes(Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> listId = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement("select athleteId from participate");
            ResultSet results = pstmt.executeQuery();
            if(results == null || results.next() == false){
                pstmt = connection.prepareStatement("select id from Athlete where id<>"+athleteId+"ORDER BY id ASC LIMIT 10");
                results = pstmt.executeQuery();

            }else {
                pstmt = connection.prepareStatement("SELECT id FROM ( "
                        + "SELECT id, COUNT(sportId) as count, athleteId FROM Athlete, participate" + " WHERE (id <> "
                        + athleteId + " AND sportId IN (SELECT sportId FROM participate WHERE athleteId=" + athleteId
                        + " )  " + " ) GROUP BY id, athleteId HAVING (id=athleteId)) as helpclose"
                        + " WHERE ( (count >= ( (0.50) * (SELECT countthis FROM ("
                        + " SELECT id, COUNT(sportId) as countthis, athleteId FROM Athlete, participate" + " WHERE (id = "
                        + athleteId
                        + " ) GROUP BY id, athleteId HAVING (id=athleteId)) as countthisid ))))  ORDER BY id ASC LIMIT 10");

                results = pstmt.executeQuery();
            }
            if (results == null || results.next() == false) {
                return listId;
            } else {
                Integer ids = results.getInt("id");
                while (ids != null) {
                    listId.add(ids);
                    // results.next();
                    if (results.next() == false)
                        break;
                    ids = results.getInt("id");
                }
            }

            results.close();
        } catch (SQLException e) {
//			System.out.print(e);
            return listId;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return listId;
    }

    public static ArrayList<Integer> getSportsRecommendation(Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> listId = new ArrayList<Integer>();
        try {
            pstmt = connection.prepareStatement("CREATE VIEW tempTable AS SELECT id FROM ( "
                    + "SELECT id, COUNT(sportId) as count, athleteId FROM Athlete, participate" + " WHERE (id <> "
                    + athleteId + " AND sportId IN (SELECT sportId FROM participate WHERE athleteId=" + athleteId
                    + " )  " + " ) GROUP BY id, athleteId HAVING (id=athleteId)) as helpclose"
                    + " WHERE ( (count >= ( (0.50) * (SELECT countthis FROM ("
                    + " SELECT id, COUNT(sportId) as countthis, athleteId FROM Athlete, participate" + " WHERE (id = "
                    + athleteId
                    + " ) GROUP BY id, athleteId HAVING (id=athleteId)) as countthisid ))))  ORDER BY id ASC LIMIT 10");

             pstmt.execute();
            pstmt = connection.prepareStatement(
                    "SELECT id FROM (SELECT id, COUNT(athleteId) as countreco FROM Sport,participate"
                            + "  WHERE (athleteId IN (SELECT id FROM tempTable) "
                            + " AND id IN (SELECT sportId FROM participate WHERE athleteId IN (SELECT id FROM tempTable) "
                            + " ) AND id NOT IN (SELECT sportId FROM participate WHERE athleteId=" + athleteId
                            + " ) AND id=sportId"

                            + " ) GROUP BY id) as reco ORDER BY countreco DESC, id ASC LIMIT 3");

            ResultSet results = pstmt.executeQuery();

            if (results == null || results.next() == false) {
                pstmt = connection.prepareStatement("DROP VIEW IF EXISTS tempTable");
                pstmt.execute();
                return listId;
            } else {
                Integer ids = results.getInt("id");
                while (ids != null) {
                    listId.add(ids);
                    if (results.next() == false)
                        break;
                    ids = results.getInt("id");
                }
                results.close();
                pstmt = connection.prepareStatement("DROP VIEW IF EXISTS tempTable");
                pstmt.execute();

            }


        }catch (SQLException e) {
//			System.out.print(e);
            return listId;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return listId;
    }
}


