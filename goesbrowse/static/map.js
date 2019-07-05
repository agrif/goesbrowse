function mapSetup(baseId, mapId) {
    var base = document.getElementById(baseId);
    var map = document.getElementById(mapId);
    
    function resize() {
        map.style.position = "absolute";
        map.style.zIndex = "1";
        map.style.left = base.offsetLeft + "px";
        map.style.top = base.offsetTop + "px";
        map.width = map.style.width = base.offsetWidth;
        map.height = map.style.height = base.offsetHeight;
    }

    window.addEventListener("resize", function() {
        resize();
    });
    resize();
}
