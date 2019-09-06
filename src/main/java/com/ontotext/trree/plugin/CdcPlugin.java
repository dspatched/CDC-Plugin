package com.ontotext.trree.plugin;

import com.ontotext.trree.sdk.*;
import com.ontotext.trree.sdk.Statements.Listener;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;


public class CdcPlugin extends PluginBase implements UpdateInterpreter {

  private static final String PREFIX = "http://example.com/";

  // IDs of the entities in the entity pool
  private long ids;

  @Override
  public String getName() {
    return "CDC-Plugin";
  }

  // Plugin interface methods
  @Override
  public void initialize(InitReason reason) {
    IRI iri = SimpleValueFactory.getInstance().createIRI(PREFIX);
    ids = getEntities().put(iri, Entities.Scope.SYSTEM);
    getLogger().info("CDC plugin initialized!");
  }

  // UpdateInterpreter interface methods
  @Override
  public long[] getPredicatesToListenFor() {
    return new long[] {ids};
  }

  @Override
  public boolean interpretUpdate(long subject, long predicate, long object, long context, boolean isAddition, boolean isExplicit, Statements statements, Entities entities) {

    Listener listener = new Listener() {

      @Override
      public boolean statementAdded(long l, long l1, long l2, long l3, long l4, boolean b) {
        String res = "Added: " + subject + " " + predicate + " " + object;
        System.out.println(res);
        getLogger().info(res);
        return false;
      }

      @Override
      public boolean statementRemoved(long l, long l1, long l2, long l3, long l4, boolean b) {
        String res = "Removed: " + subject + " " + predicate + " " + object;
        System.out.println(res);
        getLogger().info(res);
        return false;
      }

      @Override
      public void transactionStarted(long tid) {
        String res = "Started transaction " + tid;
        System.out.println("Started transaction " + tid);
        getLogger().info(res);
      }

      @Override
      public void transactionCompleted(long tid) {
        String res = "Finished transaction " + tid;
        System.out.println(res);
        getLogger().info(res);
      }

      @Override
      public void transactionAborted(long tid) {
        String res = "Aborted transaction " + tid;
        System.out.println(res);
        getLogger().info(res);
      }
    };

    statements.addListener(listener);
    return false;
  }

}
