package ra;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

@org.junit.runner.RunWith(org.junit.runners.JUnit4.class)
public class ClientTest {

    final Client ra = new Client("localhost", 8080, 4, 5000);

    @org.junit.Before
    public void testReset() throws IOException, ExecutionException, InterruptedException {
        Future<Client.Result> f = ra.reset();
        Client.Result r = f.get();
        assertEquals(true, r.success);
        assertEquals(200, r.code);
        System.out.printf("redis-es cleared in %dms%n", r.elapsedNanos / 1000000);
    }

    private String aboutCat = "Like a black cat in the dark room";
    private String what = "black cat";
    private int n = 10;

    @Test
    public void testPut() throws IOException, ExecutionException, InterruptedException {
        List<Future<Client.Result>> f = new ArrayList<Future<Client.Result>>(n);
        for (int i = 0; i < n; ++i)
            f.add(ra.put("A" + i, aboutCat));
        for (int i = 0; i < n; ++i)
            assertEquals(201, f.get(i).get().code);
    }

    @Test
    public void testSearch() throws IOException, ExecutionException, InterruptedException {
        testReset();
        testPut();
        Client.Result r = ra.search(what).get();
        assertEquals(200, r.code);
        assertEquals(n, r.ids.size());
    }

    @Test
    public void testDelete() throws IOException, ExecutionException, InterruptedException {
        testReset();
        testPut();
        assertEquals(200, ra.remove(what).get().code);
        Client.Result r = ra.search(what).get();
        assertEquals(200, r.code);
        assertEquals(0, r.ids.size());
    }

    @Test
    public void testDeleteByIds() throws IOException, ExecutionException, InterruptedException {
        testReset();
        testPut();
        Client.Result r = ra.search(what).get();
        assertEquals(200, r.code);
        assertEquals(n, r.ids.size());
        String[] result = r.ids.toArray(new String[n]);
        Arrays.sort(result);
        List<String> toRemove = Arrays.asList(Arrays.copyOf(result, n-1));
        assertEquals(200, ra.remove(toRemove).get().code);
        Client.Result r2 = ra.search(what).get();
        assertEquals(200, r2.code);
        assertEquals(1, r2.ids.size());
        assertArrayEquals(new String[] { "A" + (n-1) }, r2.ids.toArray());
    }
}
