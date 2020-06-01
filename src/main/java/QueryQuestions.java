import Client.OrientDBClient;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;


class QueryQuestions {


    String query;

    OrientDBClient orient = new OrientDBClient(false);
    ODatabaseSession db = orient.getDB();

    public static void main(String[] args) {
        QueryQuestions qq = new QueryQuestions();
        String clientId = "2199023256728";
        String productId = "B0000568SY";
        String marque = "Franklin Sports";
        qq.question1(clientId);
        qq.question2(productId, "2018");
        qq.question3(productId);
        qq.question4();
        qq.question5(clientId, marque);
        qq.question6(clientId, "4398046521398");
        qq.question7(marque);
        qq.question8("2018");
        qq.question9();
        qq.question10();
    }

    /* For a given customer,
     find his/her all related data including profile,
     orders, invoices, feedback, comments, and posts in the last month,
     return the category in which he/she has bought the largest number of products,
     and return the tag which he/she has engaged the greatest times in the posts.
    */

    void question1(String clientId) {
        query = "Select $client,$orders,$feedback,$posts,$list1,$list2 "
                + "let $client=(select from `Person` where id=?),"
                + "$orders=(select Expand(Order) from `Person` where id=?),"
                + "$feedback=(select Expand(Feedback) from `Person` where id=?),"
                + "$posts= (select Out(\'PersonGaveFeedback\') from `Person` where id=?),"
                + "$list1= (select list.Vendor as brand, count(list.Vendor) as cnt from (select Order.Orderline as list from `Person` where id=? unwind list) group by list.Vendor ORDER BY cnt DESC),"
                + "$list2=(select pid, count(pid) from (select Out(\'PersonHasPost\').Out(\'PostHasTag\').productId as pid from `Customer` where id=? unwind pid) group by pid order by count Desc)";

        OResultSet rs = db.query(query, clientId, clientId, clientId, clientId, clientId, clientId);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    /* For a given product during a given period, find the people who commented or posted on it, and had bought it. */

    void question2(String productId, String year) {
        query = "Select $client let $list=(select In(\'PostIsLabelledByTag \').In(\'PostIsLabelledByTag \').id as clientId "
                + "from `Vendor` where rid=?),$client =(select PersonId,Orderline.productId from Order "
                + "where OrderDate> ? and PersonId in $list and ? in Orderline.productId)";

        OResultSet rs = db.query(query, productId, year, productId);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    /*
    For a given product during a given period,
     find people who have undertaken activities related to it, e.g.,
     posts, comments, and review,
     and return sentences from these texts that contain negative sentiments.
     */

    // A Modifier avec la période
    void question3(String productId) {
        query = "Select $post,$feedback "
                + "let $post=(select Expand(In(\'PersonHasInterestInTag \')) from `Product` "
                + "where productId=?),"
                + "$feedback=(select * from `Feedback` where asin=? and feedback.charAt(1).asInteger() <3)";
        OResultSet rs = db.query(query, productId, productId);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }
    }

    /*
    Find the top-2 persons who spend the highest amount of money in orders.
    Then for each person, traverse her knows-graph with 3-hop to find the friends,
    and finally return the common friends of these two persons.
     */

    void question4() {
        String query = "SELECT commonset.size() from (SELECT intersect($set1,$set2) as commonset "
                + "let $person = (select clientId from (select PersonId as clientId, SUM(TotalPrice) as sum from Order Group by PersonId order by sum desc limit 2)),"
                + "$set1=(TRAVERSE out(\"PersonKnowsPerson \") FROM (select from Person where PersonId=$person.pid[0]) while $depth <= 3 STRATEGY BREADTH_FIRST),"
                + "$set2=(TRAVERSE out(\"PersonKnowsPerson \") FROM (select from Person where PersonId=$person.pid[1]) while $depth <= 3 STRATEGY BREADTH_FIRST))";
        OResultSet rs = db.query(query);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }
    }

    /*
    Given a start customer and a product category,
     find persons who are this customer's friends within 3-hop friendships in Knows graph,
     besides, they have bought products in the given category.
     Finally, return feedback with the 5-rating review of those bought products.
     */

    void question5(String clientId, String marque) {
        String query = "Select Out(\'PersonHasPost\').Out(\'PostIsLabelledByTag\') " +
                "as tags from (select Expand(Out(\'Knows\')) from Person where id=?) " +
                "Where ? in Order.Orderline.brand unwind tags," +
                "$feedback=(select * from `Feedback` where feedback.charAt(1).asInteger() ==5)";

        OResultSet rs = db.query(query, clientId, marque);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    /*
    Given customer 1 and customer 2, find persons in the shortest path between them in the subgraph,
    and return the TOP 3 best sellers from all these persons' purchases.
     */

    void question6(String clientId1, String clientId2) {
        query = "SELECT transactions, count(transactions) as cnt "
                + "FROM(SELECT Order.Orderline.productId as transactions from(SELECT EXPAND(path) from(SELECT shortestPath($from, $to) AS path "
                + "LET $from = (SELECT FROM Person WHERE id=?),"
                + "$to = (SELECT FROM Person WHERE id=?))) unwind transactions) GROUP BY transactions Order by cnt DESC LIMIT 3";
        OResultSet rs = db.query(query, clientId1, clientId2);

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    /*
    For the products of a given vendor with declining sales compare to the former quarter,
     analyze the reviews for these items to see if there are any negative sentiments.
     */

    void question7(String brand) {
        //TODO problème on a pas réussi à anlyse the review avec le sentiments négatif. On aurai put prendre en compte la note mais ca ne représente pas forcément un avis négatif
    }

    /*
    For all the products of a given category during a given year, compute its total sales amount,
     and measure its popularity in the social media.
     */

    // Ajoutez en fonction d'une date
    void question8(String year) {
        query = "Select Sum(Popularity) from(Select In(\'PostHasTag\').size() as Popularity "
                + "from `Product` Where productId in (Select  Distinct(Orderline.productId) "
                + "From (Select Orderline From Order let  $brand=(select name as brand from `Vendor` where country='China') "
                + "Where OrderDate>? and OrderDate<? unwind Orderline) Where Orderline.brand in $brand.brand))";
        int year2 = Integer.parseInt(year) + 1;
        OResultSet rs = db.query(query, year, String.valueOf(year2));

        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }
    /*
    Find top-3 companies who have the largest amount of sales at one country, for each company,
    compare the number of the male and female customers, and return the most recent posts of them.
     */

    void question9() {
        query = "Select Orderline.brand, count(*) from(Select PersonId, Orderline From Order "
                + "Let  $brand=(select name as brand from `Vendor` where country='France') Where OrderDate>\"2017\" and OrderDate<\"2018\" unwind Orderline) "
                + "Where Orderline.brand in $brand.brand Group by Orderline.brand Order by count DESC LIMIT 3";
        OResultSet rs = db.query(query);
        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }

    /*
    Find the top-10 most active persons by aggregating the posts during the last year,
    then calculate their RFM (Recency, Frequency, Monetary) value in the same period,
     and return their recent reviews and tags of interest.
     */

    void question10() {
        query = "SELECT id, max(Order.OrderDate) as Recency,Order.size() as Frequency,sum(Order.TotalPrice) as Monetary FROM Person "
                + "Where id in(Select id, count(id) as cnt from (Select IN(\'PersonGaveFeedback\').id[0] as id From Post "
                + "Where creationDate>= date( \'2012-04-08\', \'yyyy-MM-dd\')) Group by id  Order by cnt DESC limit 10) GROUP BY id";
        OResultSet rs = db.query(query);
        while (rs.hasNext()) {
            OResult item = rs.next();
            System.out.println(item);
        }

        rs.close();
        orient.close();
    }
}