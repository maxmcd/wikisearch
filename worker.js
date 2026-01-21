export default {
  async fetch(request, env) {
    const response = await env.ASSETS.fetch(request);
    
    if (new URL(request.url).pathname.endsWith('.gz')) {
      const headers = new Headers(response.headers);
      headers.set('Content-Encoding', 'gzip');
      return new Response(response.body, {
        status: response.status,
        headers
      });
    }
    
    return response;
  }
};
