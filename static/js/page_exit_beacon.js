// DataSolveX: Best-effort exit capture (tab close/navigation) using sendBeacon.
(function(){
  function fire(){
    try{ if(navigator.sendBeacon){ navigator.sendBeacon('/log-exit-beacon','1'); } }catch(e){}
  }
  window.addEventListener('pagehide', fire, {capture:true});
})();
