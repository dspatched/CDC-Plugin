package com.ontotext.trree.plugin;

import com.ontotext.trree.sdk.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.util.iterators.SingletonIterator;

import java.util.Iterator;

public class CdcPlugin extends PluginBase implements PatternInterpreter, Preprocessor, Postprocessor, Configurable {

  private static final String PREDICATE_PARAM = "predicate-uri"; // parameter name

  // Service interface methods
  @Override
  public String getName() {
    return "example";
  }

  // Plugin interface methods
  @Override
  public void initialize(InitReason reason) {
    getLogger().info("CDC plugin initialized!");
  }

  @Override
  public StatementIterator interpret(long subject, long predicate, long object, long context,
      Statements statements, Entities entities, RequestContext requestContext) {
    return null;
  }

  @Override
  public double estimate(long subject, long predicate, long object, long context,
      Statements statements, Entities entities, RequestContext requestContext) {
    // We always return a single statement
    return 1;
  }

  // Preprocessor interface methods
  @Override
  public RequestContext preprocess(Request request) {
    if (request instanceof UpdateRequest) {
      UpdateRequest updateRequest = (UpdateRequest) request;
      getLogger().info(updateRequest.getUpdateExpr().toString());
    }
    if (request instanceof QueryRequest) {
      QueryRequest queryRequest = (QueryRequest) request;
      getLogger().info(queryRequest.getTupleExpr().toString());
    }
    return null;
  }

  // Postprocessor interface methods
  @Override
  public boolean shouldPostprocess(RequestContext requestContext) {
    return false;
  }

  @Override
  public BindingSet postprocess(BindingSet bindingSet, RequestContext requestContext) {
    return null;
  }

  @Override
  public Iterator<BindingSet> flush(RequestContext requestContext) {
    // return the binding set we have created in the preprocess phase
    BindingSet result = ((Context) requestContext).getResult();
    return new SingletonIterator<>(result);
  }

  // Configurable interface methods
  @Override
  public String[] getParameters() {
    // GraphDB should expect our parameter
    return new String[] { PREDICATE_PARAM };
  }

  /**
   * Context where we can store data during the processing phase
   */
  private static class Context implements RequestContext {
    private Request theRequest;
    private BindingSet theResult;

    public Context(BindingSet result) {
      theResult = result;
    }

    @Override
    public Request getRequest() {
      return theRequest;
    }

    @Override
    public void setRequest(Request request) {
      theRequest = request;
    }

    public BindingSet getResult() {
      return theResult;
    }
  }
}
