import CRUD.Customer;
import Client.OrientDBClient;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;


class QueryQuestions {
    String query;

    OrientDBClient orient = new OrientDBClient();
    ODatabaseSession db = orient.getDB();

    void question1(String clientId) {
        query = "Select $profile,$orders,$feedback,$posts,$list1,$list2 "
                + "let $profile=(select from `Customer` where id=?),"
                + "$orders=(select Expand(Order) from `Customer` where id=?),"
                + "$feedback=(select Expand(Feedback) from `Customer` where id=?),"
                + "$posts= (select Out(\'PersonHasPost\') from `Customer` where id=?),"
                + "$list1= (select list.brand as brand, count(list.brand) as cnt from (select Order.Orderline as list from `Customer` where id=? unwind list) group by list.brand ORDER BY cnt DESC),"
                + "$list2=(select pid, count(pid) from (select Out(\'PersonHasPost\').Out(\'PostHasTag\').productId as pid from `Customer` where id=? unwind pid) group by pid order by count Desc)";

        OResultSet rs = db.query(query, clientId, clientId, clientId, clientId, clientId, clientId);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    void question2(String productId) {
        query = "Select $person let $list=(select In(\'PostHasTag\').In(\'PersonHasPost\').id as pid "
                + "from `Product` where productId=?),$person=(select PersonId,Orderline.productId from Order "
                + "where OrderDate>\"2022\" and PersonId in $list and ? in Orderline.productId)";

        OResultSet rs = db.query(query, productId, productId);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    void question3(String productId) {
        query = "Select $post,$feedback "
                + "let $post=(select Expand(In(\'PostHasTag\')) from `Product` "
                + "where productId=?),"
                + "$feedback=(select * from `Feedback` where asin=? and feedback.charAt(1).asInteger() <5)";
        OResultSet rs = db.query(query, productId, productId);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }
    }

    void question4() {
        String query = "SELECT commonset.size() from (SELECT intersect($set1,$set2) as commonset "
                + "let $person = (select pid from (select PersonId as pid, SUM(TotalPrice) as sum from Order Group by PersonId order by sum desc limit 2)),"
                + "$set1=(TRAVERSE out(\"Knows\") FROM (select from Customer where PersonId=$person.pid[0]) while $depth <= 3 STRATEGY BREADTH_FIRST),"
                + "$set2=(TRAVERSE out(\"Knows\") FROM (select from Customer where PersonId=$person.pid[1]) while $depth <= 3 STRATEGY BREADTH_FIRST))";
        OResultSet rs = db.query(query);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }
    }


    void question5(String clientId, String marque) {
        String query = "Select Out(\'PersonHasPost\').Out(\'PostHasTag\') " +
                "as tags from (select Expand(Out(\'Knows\')) from Customer where id=?) " +
                "Where ? in Order.Orderline.brand unwind tags";

        OrientDBClient orient = new OrientDBClient();
        ODatabaseSession db = orient.getDB();

        OResultSet rs = db.query(query, clientId, marque);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    void question6(String customer1, String customer2) {
        query = "SELECT transactions, count(transactions) as cnt "
                + "FROM(SELECT Order.Orderline.productId as transactions from(SELECT EXPAND(path) from(SELECT shortestPath($from, $to) AS path "
                + "LET $from = (SELECT FROM Customer WHERE id=?),"
                + "$to = (SELECT FROM Customer WHERE id=?))) unwind transactions) GROUP BY transactions Order by cnt DESC LIMIT 5";
        OResultSet rs = db.query(query, customer1, customer2);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    void question7(String brand) {
        query = "Select feedback from Feedback where asin in "
                + "(Select dlist from(Select set(dlist) as dlist  "
                + "from(Select $declineList.asin as dlist "
                + "let $list1 = (Select asin,count(asin) as cnt from "
                + "(Select ol_unwind.asin as asin, ol_unwind.brand as brand from "
                + "(Select Orderline as ol_unwind from (Select From Order Where OrderDate>\"2018\" and OrderDate<\"2019\" and ? in Orderline.brand) unwind ol_unwind)) "
                + "where brand=? group by asin order by cnt DESC), "
                + "$list2=(Select asin,count(asin) as cnt from (Select ol_unwind.asin as asin, ol_unwind.brand as brand from "
                + "(Select Orderline as ol_unwind from (Select From Order Where OrderDate>\"2019\" and OrderDate<\"2020\" and ? in Orderline.brand) unwind ol_unwind)) "
                + "where brand=? group by asin order by cnt DESC), $declineList=compareList($list1,$list2))))";
        OResultSet rs = db.query(query, brand, brand, brand, brand);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    void question8() {
        String OQ8 = "Select Sum(Popularity) from(Select In(\'PostHasTag\').size() as Popularity "
                + "from `Product` Where productId in (Select  Distinct(Orderline.productId) "
                + "From (Select Orderline From Order let  $brand=(select name as brand from `Vendor` where country='China') "
                + "Where OrderDate>\"2018\" and OrderDate<\"2019\" unwind Orderline) Where Orderline.brand in $brand.brand))";
        OResultSet rs = db.query(query);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    void question9() {
        query = "Select Orderline.brand, count(*) from(Select PersonId, Orderline From Order "
                + "Let  $brand=(select name as brand from `Vendor` where country='China') Where OrderDate>\"2018\" and OrderDate<\"2019\" unwind Orderline) "
                + "Where Orderline.brand in $brand.brand Group by Orderline.brand Order by count DESC LIMIT 3";
        OResultSet rs = db.query(query);
        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    void question10() {
        query = "SELECT id, max(Order.OrderDate) as Recency,Order.size() as Frequency,sum(Order.TotalPrice) as Monetary FROM Customer "
                + "Where id in(Select id, count(id) as cnt from (Select IN(\'PersonHasPost\').id[0] as id From Post "
                + "Where creationDate>= date( \'2012-10-01\', \'yyyy-MM-dd\')) Group by id  Order by cnt DESC limit 10) GROUP BY id";
        OResultSet rs = db.query(query);
        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }
}